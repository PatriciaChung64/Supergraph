#include <stdio.h>
#include <stdlib.h>
#include "supergraph.h"

#ifndef QUEUE_H
#define QUEUE_H

//Each node within the queue will be an instance of struct node
struct node {
  /**
  NOTE: These parameters are only for shortest_user_link
  Will always be null when queue is used for other functions
  
  @param size_t *path_idx: array of indexs in user array
  		that fomulates the current path
  @param size_t num_steps: number of steps in current path, 
  		includes start user
  @param struct user cur_user: element from the user array 
  		that current node corresponds to
  **/
  size_t *path_idxs;
  size_t num_steps;
  struct user cur_user;

  /**
  NOTE: These parameters are only for find_all_reposts
  Will always be null when queue is used otherwise
  
  @param struct result *result: result struct created for return in
  		find_all_reposts, contains an array of elements which are reposts
		of the target post, and number of elements in that array
  @param post cur_post: element from the post array 
  		that current node corresponds to
  **/
  struct post cur_post;
  struct post **elements;
  size_t n_elements;

  /**
  common parameters => will have value regardless of use in functions
  @param struct node *next: the next node in queue
  @param struct node *prev: the previous node in queue
  @size_t index: the index the current element, either cur_user or cur_post,
  		has within their respective users/posts list
  **/
  struct node *next;
  struct node *prev;
  size_t index;
};

/**
Queue specific variables
@param num_in_queue: number of elements currently in queue, 
		excludes head and tail
@param struct node *head: head of queue, used to keep track of first element
@param struct node *tail: end of queue, used to keep track of last element
**/
int num_in_queue = 0;
struct node *head;
struct node *dummy;

/**
Add an element to end of queue
@param struct node * n: element to be added
@param int *visit_status: the visited status of the elements within the node,
		visited_status[n->index] = 0 =>unvisited;
		visited_status[n->index] = 1 =>visited.
		
        IF node has already been visited (1), 
	    	free the current node and return without enqueuing.
	   
	    ELSE enqueue node and change corresponding 
	   		index in visited_status to 1 (visited)
@return void
**/
void enqueue(struct node *n, int *visit_status) {
  if (num_in_queue == 0) {
    head->path_idxs = NULL;
    head->next = dummy;
    head->prev = NULL;

    dummy->path_idxs = NULL;
    dummy->next = NULL;
    dummy->prev = head;
  }
  
  if (visit_status[n->index] == 1) {
	  if (n->path_idxs != NULL) {
		  free(n->path_idxs);
	  }
	  free(n);
      return;
  }
    
  else {
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

    num_in_queue++;
    visit_status[n->index] = 1;
    return;
  }
}

/**
Retrieve the element at the start of the queue (oldest element added)
@param void
@return struct node *n: element at start of queue
**/
struct node *dequeue() {
  struct node *result = dummy->next;
  dummy->next = dummy->prev;
  if(num_in_queue > 1) {
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
  num_in_queue--;
  return result;
}

/**
Clear up queue until no more elements in queue
@param void
@return void
**/
void clear_queue() {
    //Dequeue and free nodes until queue is empty (num_in_queue == 0)
	while (num_in_queue > 0) {
		struct node *node = dequeue();
		if(node->path_idxs != NULL) {
			free(node->path_idxs);
		}
		free(node);
	}
}

#endif
