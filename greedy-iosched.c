/*
 * elevator greedy
 */
#include <linux/blkdev.h>
#include <linux/elevator.h>
#include <linux/bio.h>
#include <linux/module.h>
#include <linux/slab.h>
#include <linux/init.h>

struct greedy_data {
	struct list_head upstream_queue;
	struct list_head downstream_queue;
	sector_t         head_position;
};

static void greedy_merged_requests(struct request_queue *q, struct request *rq,
				 struct request *next)
{
	list_del_init(&next->queuelist);
}

static int greedy_dispatch(struct request_queue *q, int force)
{
	struct request     *rq   = NULL; // next request
	struct request     *rq_u = NULL; // upstream request
	struct request     *rq_d = NULL; // downstream request
	sector_t            hp   = 0;    // next head position
	sector_t            hp_u = 0;    // upstream head position
	sector_t            hp_d = 0;    // downstream head position
	struct greedy_data *gd   = q->elevator->elevator_data;
	
	if (!list_empty(&gd->upstream_queue)) 
		{
		rq_u = list_entry(gd->upstream_queue.next, struct request, queuelist);
		hp_u = blk_rq_pos( rq_u );
		rq   = rq_u;
		hp   = hp_u;
		}
	
	if (!list_empty(&gd->downstream_queue)) 
		{
		rq_d = list_entry(gd->downstream_queue.next, struct request, queuelist);
		hp_d = blk_rq_pos( rq_d );
		if( rq == NULL )
			{
			rq = rq_d;
			hp = hp_d;
			}
		}
	
	if( rq )
		{
		// if a request is available upstream and downstream,
		// then rq and hp only need to be change if the the downstream head position is closer
		// since it will have already been set to the upstream request previously
		if( rq_d && rq_u && ( (gd->head_position - hp_d) < (hp_u - gd->head_position) ) )
			{
			rq = rq_d;
			hp = hp_d;
			}
		list_del_init(&rq->queuelist);
		gd->head_position = hp;
		elv_dispatch_sort(q, rq);
		return 1;
		}
	
	return 0;
}

static void greedy_add_request(struct request_queue *q, struct request *rq)
{
	struct greedy_data *gd = q->elevator->elevator_data;
	sector_t            hp = blk_rq_pos( rq );    // head position of the request
	struct list_head   *iter;
	
	// use the upstream queue if the request is after the current head position
	if( hp > gd->head_position )
		{
		list_for_each( iter, &gd->upstream_queue )
			{
			if( hp - gd->head_position  < 
			    blk_rq_pos( list_entry(iter, struct request, queuelist) ) - gd->head_position )
				{
				break;
				}
			}
		}
	else
		{
		list_for_each( iter, &gd->downstream_queue )
			{
			if( gd->head_position - hp < 
			    gd->head_position - blk_rq_pos( list_entry(iter, struct request, queuelist) ) )
				{
				break;
				}
			}
		}

	list_add_tail( &rq->queuelist, iter );
}

static struct request *
greedy_former_request(struct request_queue *q, struct request *rq)
{
	struct greedy_data *gd = q->elevator->elevator_data;

	if ( rq->queuelist.prev == &gd->upstream_queue 
	  || rq->queuelist.prev == &gd->downstream_queue )
		return NULL;
	return list_entry(rq->queuelist.prev, struct request, queuelist);
}

static struct request *
greedy_latter_request(struct request_queue *q, struct request *rq)
{
	struct greedy_data *gd = q->elevator->elevator_data;

	if ( rq->queuelist.next == &gd->upstream_queue 
	  || rq->queuelist.next == &gd->downstream_queue )
		return NULL;
	return list_entry(rq->queuelist.next, struct request, queuelist);
}

static int greedy_init_queue(struct request_queue *q, struct elevator_type *e)
{
	struct greedy_data *gd;
	struct elevator_queue *eq;

	eq = elevator_alloc(q, e);
	if (!eq)
		return -ENOMEM;

	gd = kmalloc_node(sizeof(*gd), GFP_KERNEL, q->node);
	if (!gd) {
		kobject_put(&eq->kobj);
		return -ENOMEM;
	}
	eq->elevator_data = gd;

	INIT_LIST_HEAD(&gd->upstream_queue);
	INIT_LIST_HEAD(&gd->downstream_queue);
	gd->head_position = 0;
	
	spin_lock_irq(q->queue_lock);
	q->elevator = eq;
	spin_unlock_irq(q->queue_lock);
	return 0;
}

static void greedy_exit_queue(struct elevator_queue *e)
{
	struct greedy_data *gd = e->elevator_data;
	BUG_ON(!list_empty(&gd->upstream_queue));
	BUG_ON(!list_empty(&gd->downstream_queue));
	kfree(gd);
}

static struct elevator_type elevator_greedy = {
	.ops = {
		.elevator_merge_req_fn		= greedy_merged_requests,
		.elevator_dispatch_fn		= greedy_dispatch,
		.elevator_add_req_fn		= greedy_add_request,
		.elevator_former_req_fn		= greedy_former_request,
		.elevator_latter_req_fn		= greedy_latter_request,
		.elevator_init_fn		= greedy_init_queue,
		.elevator_exit_fn		= greedy_exit_queue,
	},
	.elevator_name = "greedy",
	.elevator_owner = THIS_MODULE,
};

static int __init greedy_init(void)
{
	return elv_register(&elevator_greedy);
}

static void __exit greedy_exit(void)
{
	elv_unregister(&elevator_greedy);
}

module_init(greedy_init);
module_exit(greedy_exit);


MODULE_AUTHOR("Prateek Bhavya Pratyush Manasa David");
MODULE_LICENSE("Proprietary");
MODULE_DESCRIPTION("Greedy IO scheduler");
