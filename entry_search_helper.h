#include "queue_threads.h"
#include "supergraph.h"
#include "supergraph_worker.h"
#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <semaphore.h>
#include <string.h>
#include <stdbool.h>

#ifndef ENTRY_SEARCH_H
#define ENTRY_SEARCH_H

size_t *divideWork(size_t size, size_t n_threads) {
	int leftover = 0;

	while ((size%n_threads) != 0) {
		size--;
		leftover++;
	}

	size_t *divided = (size_t *)malloc(n_threads * sizeof(size_t));
	for (int i = 0; i < n_threads; i++) {
		divided[i] = size/n_threads;
	}
	int idx = 0;
	while (leftover > 0) {
		if (idx > (n_threads-1)) {
			idx = 0;
		}
		divided[idx] += 1;
		idx++;
		leftover--;		
	}

	return divided;
}

int find_target_idx_p(query_helper *helper, size_t *dividedWork, void *elements, uint64_t target) {
	size_t offset = 0;
	struct threads **thread_info = helper->thread_info;
	uint64_t *target_ptr = (uint64_t *)malloc(sizeof(uint64_t));
	target_ptr[0] = target;
	for (int i = 0; i < helper->n_threads; i++) {
		if (thread_info[i]->search_info != NULL) {
			free(thread_info[i]->search_info);
			thread_info[i]->search_info = NULL;
		}
		struct threads *cur_thread = thread_info[i];
		struct entry_search *search_info = (struct entry_search *)malloc(sizeof(struct entry_search));
		search_info->start = offset;
		offset += dividedWork[i];
		search_info->end = offset;
		search_info->elements = elements;
		search_info->target = target_ptr;
		
		cur_thread->search_info = (void *)search_info;
		cur_thread->sig = SIGSEARCH_P;

		sem_post(helper->work_status);
	}

	while(1) {
		int checkDone = 0;
		sem_getvalue(helper->work_status, &checkDone);
		if (checkDone == 1) {
			break;
		}
	}
	free(target_ptr);

	size_t *result = NULL;
	for (int i = 0; i < helper->n_threads; i++) {
		if (((helper->thread_info)[i])->query_result != NULL) {
			result = (size_t *)((helper->thread_info)[i])->query_result;
			((helper->thread_info)[i])->query_result = NULL;
		}
	}

	if (result == NULL) {
		return -1;
	}
	else {
		size_t returnval = *(result);
		free(result);
		return returnval;
	}
}

size_t *find_target_idx_u(query_helper *helper, size_t *dividedWork, void *elements, uint64_t target1, uint64_t target2) {
	size_t offset = 0;
	uint64_t *target = (uint64_t *)malloc(2 * sizeof(uint64_t));
	target[0] = target1;
	target[1] = target2;
	struct threads **thread_info = helper->thread_info;
	sem_t *work_status = helper->work_status;
	for (int i = 0; i < helper->n_threads; i++) {
		if (thread_info[i]->search_info != NULL) {
			free(thread_info[i]->search_info);
			thread_info[i]->search_info = NULL;
		}
		struct entry_search *search_info = (struct entry_search *)malloc(sizeof(struct entry_search));
		search_info->start = offset;
		offset += dividedWork[i];
		search_info->end = offset;
		search_info->elements = elements;
		search_info->target = target;
		
		struct threads *cur_thread = thread_info[i];
		cur_thread->search_info = search_info;
		cur_thread->sig = SIGSEARCH_U;
		
		sem_post(work_status);
	}
	
	while(1) {
		int checkDone = 0;
		sem_getvalue(helper->work_status, &checkDone);
		if (checkDone == 1) {
			break;
		}
	}
	
	free(target);

	bool found1 = false;
	bool found2 = false;
	size_t *idxs = (size_t *)malloc(2 * sizeof(size_t));
	for (int i = 0; i < helper->n_threads; i++) {
		size_t *query_results = (size_t *)thread_info[i]->query_result;
		if (query_results != NULL) {
			if (query_results[0] == 1 && !found1) {
				idxs[0] = query_results[1];
				found1 = true;
			}
			else if (query_results[0] == 1 && found1) {
				idxs[1] = query_results[1];
				found2 = true;
			}
			else if (query_results[0] == 2) {
				idxs[0] = query_results[1];
				idxs[1] = query_results[2];
				found1 = true;
				found2 = true;
			}
			free(query_results);
			thread_info[i]->query_result = NULL;
		}
	}

	if (!found1 || !found2) {
		free(idxs);
		return NULL;
	}
	return idxs;
}

void initiate_bot_search(size_t *divided, bool *isRoot, struct criteria *crit, struct user *users, query_helper *helper) {
	struct threads **thread_info = helper->thread_info;
	size_t offset = 0;
	for (int i = 0; i < helper->n_threads; i++) {
		if (thread_info[i]->search_info != NULL) {
			free(thread_info[i]->search_info);
			thread_info[i]->search_info = NULL;
		}
		struct bot_search *search_info = (struct bot_search *)malloc(sizeof(struct bot_search));
		search_info->start = offset;
		offset += divided[i];
		search_info->end = offset;
		search_info->users = users;
		search_info->isRoot = isRoot;
		search_info->oc_threshold = crit->oc_threshold;
		search_info->acc_rep_threshold = crit->acc_rep_threshold;
		
		thread_info[i]->search_info = (void *)search_info;
		thread_info[i]->sig = SIGBOT;
	}
	
	while(1) {
		bool workdone = true;
		for (int i = 0; i < helper->n_threads; i++) {
			if (thread_info[i]->sig != 0) {
				workdone = false;
			}
		}
		if (workdone) {
			break;
		}
	}
	return;
}

void initiate_discrete_bot_search(size_t *divided, bool *isBot, float bot_net_threshold, struct user *users, query_helper *helper) {
	struct threads **thread_info = helper->thread_info;
	size_t offset = 0;
	for (int i = 0; i < helper->n_threads; i++) {
		if (thread_info[i]->search_info == NULL) {
			struct discrete_bot_search *search_info = (struct discrete_bot_search *)malloc(sizeof(struct discrete_bot_search));
			search_info->start = offset;
			offset += divided[i];
			search_info->end = offset;
			search_info->users = users;
			search_info->isBot = isBot;
			search_info->bot_net_threshold = bot_net_threshold;

			thread_info[i]->search_info = (void *)search_info;			
		}
		thread_info[i]->sig = SIGDISCRETEBOT;
	}
	
	while(1) {
		bool workdone = true;
		for (int i = 0; i < helper->n_threads; i++) {
			if (thread_info[i]->sig != 0) {
				workdone = false;
			}
		}
		if (workdone) {
			break;
		}
	}
	return;
}



#endif
