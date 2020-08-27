#ifndef QUEUE_THREAD_H
#define QUEUE_THREAD_H

#include <stdint.h>
#include <stdlib.h>
#include <semaphore.h>
#include <pthread.h>

struct link_search {
	size_t *path_idxs;
	size_t n_steps;
	size_t target;
	
	bool *queue_status;
	bool *found_sig;
	struct user *users;
	
	size_t index;
};

struct post_search {
	struct post **elements;
	size_t *n_elements;
	struct post *posts;
	size_t index;
};

struct bot_search {
	size_t start;
	size_t end;
	struct user *users;
	bool *isRoot;
	float oc_threshold;
	float acc_rep_threshold;
};

struct discrete_bot_search {
	size_t start;
	size_t end;
	struct user *users;
	bool *isBot;
	float bot_net_threshold;
};

struct entry_search {
	size_t start;
	size_t end;
	uint64_t *target;
	void *elements;
};

struct original_search {
	size_t start;
	size_t end;
	size_t *target;
	sem_t *found;
	struct post *posts;
};

struct node {
	struct node *next;
	struct node *prev;

	struct link_search *path;
	struct post_search *reposts;
};

struct queue {
	struct node *head;
	struct node *dummy;
	sem_t *num_in_queue;
};

struct queue *createQueue() {
	struct node *head = (struct node *)malloc(sizeof(struct node));
	struct node *dummy = (struct node *)malloc(sizeof(struct node));
	sem_t *num_in_queue = (sem_t *)malloc(sizeof(sem_t));
	sem_init(num_in_queue, 0, 1);

	head->next = dummy;
	head->prev = NULL;
	dummy->next = NULL;
	dummy->prev = head;

	struct queue *new_queue = (struct queue *)malloc(sizeof(struct queue));
	new_queue->head = head;
	new_queue->dummy = dummy;
	new_queue->num_in_queue = num_in_queue;

	return new_queue;
}

void enqueue(struct queue *q, struct node *n, bool *visit_status) {
	struct node *head = q->head;
	struct node *dummy = q->dummy;

	if (dummy->next == NULL) {
		dummy->next = n;
		n->next = NULL;
		n->prev = dummy;
	}
	else {
		struct node *temp = head->next;
		head->next = n;
		n->next = temp;
		n->prev = head;
		temp->prev = n;
	}

	sem_post(q->num_in_queue);
	if (visit_status != NULL) {
		if (n->reposts != NULL) {
			visit_status[n->reposts->index] = true;
		}
		else {
			visit_status[n->path->index] = true;
		}		
	}
	return;
}

int getSize(struct queue *q) {
	int size = 0;
	sem_getvalue(q->num_in_queue, &size);
	return size;
}

struct node *dequeue(struct queue *q) {
	struct node *dummy = q->dummy;
	struct node *head = q->head; 

	struct node *result = dummy->next;
	if(getSize(q) > 2) {
		dummy->next = dummy->prev;
		dummy->prev = (dummy->next)->prev;
		(dummy->prev)->next = dummy;
		(dummy->next)->prev = dummy;
	}
	else {
		dummy->next = NULL;
		dummy->prev = head;
		head->next = dummy;
		head->prev = NULL;
	}
	sem_wait(q->num_in_queue);
	return result;
}

void clear_queue(struct queue *q) {
	while (getSize(q) > 1) {
		struct node *node = dequeue(q);
		if (node->reposts != NULL) {
			free(node->reposts);
		}
		if (node->path != NULL) {
			free(node->path);
		}
		free(node);
	}
}

void queue_destroy(struct queue *q) {
	clear_queue(q);
	sem_destroy(q->num_in_queue);
	free(q->num_in_queue);
	free(q->head);
	free(q->dummy);
	free(q);
	return;
}

#endif

