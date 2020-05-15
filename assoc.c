/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Hash table
 *
 * The hash function used here is by Bob Jenkins, 1996:
 *    <http://burtleburtle.net/bob/hash/doobs.html>
 *       "By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.
 *       You may use this code any way you wish, private, educational,
 *       or commercial.  It's free."
 *
 * The rest of the file is licensed under the BSD license.  See LICENSE.
 */

#include "memcached.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

static pthread_cond_t maintenance_cond = PTHREAD_COND_INITIALIZER;


typedef  unsigned long  int  ub4;   /* unsigned 4-byte quantities */
typedef  unsigned       char ub1;   /* unsigned 1-byte quantities */

/* how many powers of 2's worth of buckets we use */
unsigned int hashpower = HASHPOWER_DEFAULT;

#define hashsize(n) ((ub4)1<<(n))
#define hashmask(n) (hashsize(n)-1)

/* Main hash table. This is where we look except during expansion. */
static item** primary_hashtable = 0;

/*
 * Previous hash table. During expansion, we look here for keys that haven't
 * been moved over to the primary yet.
 */
static item** old_hashtable = 0;

/* Number of items in the hash table. */
static unsigned int hash_items = 0;

/* Flag: Are we in the middle of expanding now? */
static bool expanding = false;
static bool started_expanding = false;

/*
 * During expansion we migrate values with bucket granularity; this is how
 * far we've gotten so far. Ranges from 0 .. hashsize(hashpower - 1) - 1.
 */
static unsigned int expand_bucket = 0;

//hashtable初始化
void assoc_init(const int hashtable_init) {
	//初始化的时候 hashtable_init值需要大于12 小于64
	//如果hashtable_init的值没有设定，则hashpower使用默认值为16
    if (hashtable_init) {
        hashpower = hashtable_init;
    }
    //primary_hashtable主要用来存储这个HashTable
    //hashsize方法是求桶的个数，默认如果hashpower=16的话，桶的个数为：65536
    primary_hashtable = calloc(hashsize(hashpower), sizeof(void *));
    if (! primary_hashtable) {
        fprintf(stderr, "Failed to init hashtable.\n");
        exit(EXIT_FAILURE);
    }
    STATS_LOCK();
    stats.hash_power_level = hashpower;
    stats.hash_bytes = hashsize(hashpower) * sizeof(void *);
    STATS_UNLOCK();
}

//首先通过key的hash值hv找到对应的桶。
//然后遍历桶的单链表，比较key值，找到对应item
item *assoc_find(const char *key, const size_t nkey, const uint32_t hv) {
    item *it;
    unsigned int oldbucket;

    //判断是否在扩容中
    if (expanding &&
        (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
    {
        it = old_hashtable[oldbucket];
    } else {
    	//获取得到具体的桶的地址
        it = primary_hashtable[hv & hashmask(hashpower)];
    }

    item *ret = NULL;
    int depth = 0; //循环的深度
    while (it) {
    	//循环查找桶的list中的Item
        if ((nkey == it->nkey) && (memcmp(key, ITEM_key(it), nkey) == 0)) {
            ret = it;
            break;
        }
        it = it->h_next;
        ++depth;
    }
    MEMCACHED_ASSOC_FIND(key, nkey, depth);
    return ret;
}

/* returns the address of the item pointer before the key.  if *item == 0,
   the item wasn't found */
//与assoc_find()一样都是查询，但是返回的是item的地址
static item** _hashitem_before (const char *key, const size_t nkey, const uint32_t hv) {
    item **pos;
    unsigned int oldbucket;

    //判断是否在扩容中
    if (expanding &&
        (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
    {
        pos = &old_hashtable[oldbucket];
    } else {
    	//返回具体桶的地址
        pos = &primary_hashtable[hv & hashmask(hashpower)];
    }

    //在桶的list中匹配key值是否相同，相同则找到Item
    while (*pos && ((nkey != (*pos)->nkey) || memcmp(key, ITEM_key(*pos), nkey))) {
        pos = &(*pos)->h_next;
    }
    return pos;
}

/* grows the hashtable to the next power of 2. */
//扩容方法
static void assoc_expand(void) {
    old_hashtable = primary_hashtable;

    primary_hashtable = calloc(hashsize(hashpower + 1), sizeof(void *)); //申请新的空间
    if (primary_hashtable) {
        if (settings.verbose > 1)
            fprintf(stderr, "Hash table expansion starting\n");
        hashpower++; //hash等级+1
        expanding = true; //扩容标识打开
        expand_bucket = 0;
        STATS_LOCK(); //更新全局统计信息
        stats.hash_power_level = hashpower;
        stats.hash_bytes += hashsize(hashpower) * sizeof(void *);
        stats.hash_is_expanding = 1;
        STATS_UNLOCK();
    } else {
        primary_hashtable = old_hashtable;
        /* Bad news, but we can keep running. */
    }
}

//Memcached的扩容都是在单独线程中进行的。
//（桶的个数(默认：65536) * 3） / 2的时候，就需要重新扩容。扩容需要的时间比较久，所以必须使用不同的线程进行自动扩容处理。
static void assoc_start_expand(void) {
    if (started_expanding)
        return;
    started_expanding = true; //标识为置为true
    //唤醒线程
    pthread_cond_signal(&maintenance_cond);
}

/* Note: this isn't an assoc_update.  The key must not already exist to call this */
//首先通过key的hash值hv找到对应的桶。
//然后将item放到对应桶的单链表的头部
//判断是否需要扩容，如果扩容，则会在单独的线程中进行。（桶的个数(默认：65536) * 3） / 2
int assoc_insert(item *it, const uint32_t hv) {
    unsigned int oldbucket;

//    assert(assoc_find(ITEM_key(it), it->nkey) == 0);  /* shouldn't have duplicately named things defined */

    //判断是否在扩容，如果是扩容中，为保证程序继续可用，则需要使用旧的桶
    if (expanding &&
        (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
    {
        it->h_next = old_hashtable[oldbucket];
        old_hashtable[oldbucket] = it;
    } else {
    	//hv & hashmask(hashpower) 按位与计算是在哪个桶上面
    	//将当前的item->h_next 指向桶中首个Item的位置
        it->h_next = primary_hashtable[hv & hashmask(hashpower)];
        //然后将hashtable中的首页Item指向新的Item地址值
        primary_hashtable[hv & hashmask(hashpower)] = it;
    }

    hash_items++;//因为是新增操作，则就会增加一个Item
    //如果hash_items的个数大于当前  （桶的个数(默认：65536) * 3） / 2的时候，就需要重新扩容
    //因为初始化的桶本身就比较多了，所以扩容必须在单独的线程中处理，每次扩容估计耗时比较长
    //优秀的设计：桶的扩容在单独的线程中处理
    if (! expanding && hash_items > (hashsize(hashpower) * 3) / 2) {
        assoc_start_expand();
    }

    MEMCACHED_ASSOC_INSERT(ITEM_key(it), it->nkey, hash_items);
    return 1;
}

//首先通过key的hash值hv找到对应的桶。
//找到桶对应的链表，遍历单链表，删除对应的Item
void assoc_delete(const char *key, const size_t nkey, const uint32_t hv) {
    item **before = _hashitem_before(key, nkey, hv); //查询Item是否存在

    //如果Item存在，则当前的Item值指向下一个Item的指针地址
    if (*before) {
        item *nxt;
        hash_items--; //item个数减去1
        /* The DTrace probe cannot be triggered as the last instruction
         * due to possible tail-optimization by the compiler
         */
        MEMCACHED_ASSOC_DELETE(key, nkey, hash_items);
        nxt = (*before)->h_next;
        (*before)->h_next = 0;   /* probably pointless, but whatever. */
        *before = nxt;
        return;
    }
    /* Note:  we never actually get here.  the callers don't delete things
       they can't find. */
    assert(*before != 0);
}


static volatile int do_run_maintenance_thread = 1;

#define DEFAULT_HASH_BULK_MOVE 1
int hash_bulk_move = DEFAULT_HASH_BULK_MOVE;

//while循环中分为两步，一个是for循环的旧桶到新桶的拷贝，另一个是等待扩容的信号
//一开始for循环中的判断条件为false，直接跳过等待扩容信号，扩容成功后再到for循环中进行桶的复制
static void *assoc_maintenance_thread(void *arg) {

    while (do_run_maintenance_thread) {
        int ii = 0;

        /* Lock the cache, and bulk move multiple buckets to the new
         * hash table. */
        //锁定缓存，然后将多个存储桶批量移动到新的哈希表
        item_lock_global();
        mutex_lock(&cache_lock);

        //执行扩容时，每次按hash_bulk_move个桶来扩容
        for (ii = 0; ii < hash_bulk_move && expanding; ++ii) {
            item *it, *next;
            int bucket;

            //老表每次移动一个桶中的一个元素
            for (it = old_hashtable[expand_bucket]; NULL != it; it = next) {
                next = it->h_next; //要移动的下一个元素

                bucket = hash(ITEM_key(it), it->nkey) & hashmask(hashpower); //按新的Hash规则进行定
                it->h_next = primary_hashtable[bucket]; //挂载到新的Hash表中
                primary_hashtable[bucket] = it;
            	}

            old_hashtable[expand_bucket] = NULL; //旧表中的这个Hash桶已经按新规则完成了扩容

            expand_bucket++; //老表中的桶计数+1
            if (expand_bucket == hashsize(hashpower - 1)) { //hash表扩容结束,expand_bucket从0开始,一直递增
                expanding = false; //修改扩容标志
                free(old_hashtable); //释放老的表结构
                STATS_LOCK(); //更新一些统计信息
                stats.hash_bytes -= hashsize(hashpower - 1) * sizeof(void *);
                stats.hash_is_expanding = 0;
                STATS_UNLOCK();
                if (settings.verbose > 1)
                    fprintf(stderr, "Hash table expansion done\n");
            }
        }

        mutex_unlock(&cache_lock); //释放cache_lock锁
        item_unlock_global(); //释放Hash表的全局锁

        if (!expanding) { //等待扩容的信号
            /* finished expanding. tell all threads to use fine-grained locks */
        	//修改Hash表的锁类型，此时锁类型更新为分段锁，默认是分段锁，在进行扩容时，改为全局锁
            switch_item_lock_type(ITEM_LOCK_GRANULAR);
            slabs_rebalancer_resume(); //释放用于扩容的锁
            /* We are done expanding.. just wait for next invocation */
            mutex_lock(&cache_lock); //加cache_lock锁，保护条件变量
            started_expanding = false; //修改扩容标识
            pthread_cond_wait(&maintenance_cond, &cache_lock); //解锁cache_lock，阻塞扩容线程等待扩容hashtable的信号maintenance_cond
            /* Before doing anything, tell threads to use a global lock */
            mutex_unlock(&cache_lock);
            slabs_rebalancer_pause();//加用于扩容的锁
            switch_item_lock_type(ITEM_LOCK_GLOBAL); //修改锁类型为全局锁
            mutex_lock(&cache_lock); //临时用来实现临界区
            assoc_expand(); //执行扩容
            mutex_unlock(&cache_lock);
        }
    }
    return NULL;
}

static pthread_t maintenance_tid;

//在main()函数中调用，单独开启一个线程准备hashtable的扩容
int start_assoc_maintenance_thread() {
    int ret;
    char *env = getenv("MEMCACHED_HASH_BULK_MOVE");
    if (env != NULL) {
        hash_bulk_move = atoi(env);
        if (hash_bulk_move == 0) {
            hash_bulk_move = DEFAULT_HASH_BULK_MOVE;
        }
    }
    if ((ret = pthread_create(&maintenance_tid, NULL,
                              assoc_maintenance_thread, NULL)) != 0) {
        fprintf(stderr, "Can't create thread: %s\n", strerror(ret));
        return -1;
    }
    return 0;
}

void stop_assoc_maintenance_thread() {
    mutex_lock(&cache_lock);
    do_run_maintenance_thread = 0;
    pthread_cond_signal(&maintenance_cond);
    mutex_unlock(&cache_lock);

    /* Wait for the maintenance thread to stop */
    pthread_join(maintenance_tid, NULL);
}


