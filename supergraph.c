#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <semaphore.h>
#include <string.h>
#include <stdbool.h>
#include <time.h>
#include "supergraph.h"
#include "queue_threads.h"
#include "supergraph_worker.h"
#include "entry_search_helper.h"

#define SIGSEARCH_U 1
#define SIGSEARCH_P 2
#define SIGREPOST 3
#define SIGLINK 4
#define SIGBOT 5
#define SIGDISCRETEBOT 6
#define SIGSTOPBOTSEARCH 7
#define SIGORIGINAL 8
#define SIGORIGINALFOUND 9
#define SIGQUIT 10

query_helper* engine_setup(size_t n_processors) {
	struct query_helper *helper = malloc(sizeof(struct query_helper));
	sem_t *work_status = malloc(sizeof(sem_t));
	sem_init(work_status, 0, 1);
	helper->work_queue = createQueue();

	pthread_t *tids = (pthread_t *)malloc(n_processors * sizeof(pthread_t));
	struct threads **thread_info = malloc(n_processors * sizeof(struct threads *));

	for (int i = 0; i < n_processors; i++) {
		struct threads *args = malloc(sizeof(struct threads));
		args->work_queue = helper->work_queue;
		args->queue_status = NULL;
		args->query_result = NULL;
		args->sig = 0;
		args->search_info = NULL;
		args->work_status = work_status;
		thread_info[i] = args;

		pthread_create(&(tids[i]), NULL, worker, (void *)args);
	}

	helper->tids = tids;
	helper->thread_info = thread_info;
	helper->n_threads = n_processors;
	helper->work_status = work_status;

	return helper;
}

result* find_all_reposts(post* posts, size_t count, uint64_t post_id, query_helper* helper) {
	struct result *returnval = malloc(sizeof(struct result));

	if (posts == NULL || count == 0) {
		returnval->elements = NULL;
		returnval->n_elements = 0;
		return returnval;
	}

	size_t *divided = divideWork(count, helper->n_threads);
	int target_idx = find_target_idx_p(helper, divided, posts, post_id);
	free(divided);

	if (target_idx == -1) {
		returnval->elements = NULL;
		returnval->n_elements = 0;
		return returnval;
	}

	bool *queue_status = calloc(count, sizeof(bool));
	struct post **elements = (struct post **)malloc(count * sizeof(struct post *));
	size_t *n_elements = calloc(1, sizeof(size_t));

	struct node *original = (struct node *)malloc(sizeof(struct node));
	original->path = NULL;
	original->prev = NULL;
	original->next = NULL;

	struct post_search *search_info = malloc(sizeof(struct post_search));
	search_info->posts = posts;
	search_info->elements = elements;
	search_info->n_elements = n_elements;
	search_info->index = target_idx;
	original->reposts = search_info;

	enqueue(helper->work_queue, original, queue_status);

	for (int i = 0; i < helper->n_threads; i++) {
		((helper->thread_info)[i])->queue_status = queue_status;
		((helper->thread_info)[i])->sig = SIGREPOST;
	}

	while (1) {
		int status = 0;
		for (int i = 0; i < helper->n_threads; i++) {
			if (helper->thread_info[i]->sig != 0) {
				status = 1;
			}
		}
		if (status == 0) {
			break;
		}
	}

	free(queue_status);

	elements = realloc(elements, ((*n_elements) * sizeof(struct post *)));
	returnval->elements = (void **)elements;
	returnval->n_elements = *(n_elements);
	free(n_elements);

	return returnval;
}

result* find_original(post* posts, size_t count, uint64_t post_id, query_helper* helper) {
	struct result *returnval = malloc(sizeof(struct result));

	if (posts == NULL || count == 0) {
		returnval->elements = NULL;
		returnval->n_elements = 0;
		return returnval;
	}

	size_t *work = divideWork(count, helper->n_threads);
	int target_idx = find_target_idx_p(helper, work, posts, post_id);

	if (target_idx == -1) {
		free(work);
		returnval->elements = NULL;
		returnval->n_elements = 0;
		return returnval;
	}

	sem_t *found = malloc(sizeof(sem_t));
	sem_init(found, 0, 1);
	size_t *target = malloc(sizeof(size_t));
	*(target) = target_idx;
	size_t offset = 0;
	for (int i = 0; i < helper->n_threads; i++) {
		struct original_search *temp = malloc(sizeof(struct original_search));
		temp->start = offset;
		temp->end = offset + work[i];
		offset += work[i];
		temp->target = target;
		temp->found = found;
		temp->posts = posts;

		((helper->thread_info)[i])->search_info = (void *)temp;
		((helper->thread_info)[i])->sig = SIGORIGINAL;
		sem_post(helper->work_status);
	}
	free(work);

	size_t old_target = target_idx;
	while(1) {
		int workdone = 0;
		sem_getvalue(helper->work_status, &workdone);

		if (workdone == 1) {
			int foundval = 0;
			sem_getvalue(found, &foundval);
			if (foundval > 1) {
				// printf("Found was %d, decreasing found\n", foundval);
				sem_wait(found);
			}
			if (old_target != *(target)) {
				// printf("Found a new original, starting new round of search\n");
				for (int i = 0; i < helper->n_threads; i++) {
					sem_post(helper->work_status);
					((helper->thread_info)[i])->sig = SIGORIGINAL;
				}
				old_target = *(target);
			}
			else {
				// printf("Did not find new original, current target is original, exiting. \n");
				for (int i = 0; i < helper->n_threads; i++) {
					((helper->thread_info)[i])->sig = SIGORIGINALFOUND;
				}
				break;
			}
		}
	}
	

	sem_destroy(found);
	free(found);

	target_idx = *(target);
	// printf("Target index now set to: %d\n", target_idx);
	free(target);

	struct post **elements = malloc(sizeof(struct post *));
	elements[0] = &(posts[target_idx]);
	returnval->elements = (void **)elements;
	returnval->n_elements = 1;

	return returnval;
}

result* shortest_user_link(user* users, size_t count, uint64_t userA, uint64_t userB, query_helper* helper) {
	struct result *returnval = malloc(sizeof(struct result));
	if (users == NULL || count == 0 || userA == userB) {
		returnval->elements = NULL;
		returnval->n_elements = 0;
		return returnval;
	}
	
	size_t *work = divideWork(count, helper->n_threads);
	size_t *target_idxs = find_target_idx_u(helper, work, (void *)users, userA, userB);
	free(work);
	if (target_idxs == NULL) {
		returnval->elements = NULL;
		returnval->n_elements = 0;
		return returnval;
	}
	size_t targetA = target_idxs[0];
	size_t targetB = target_idxs[1];
	free(target_idxs);
	
	//find A to B
	bool *queue_status = calloc(count, sizeof(bool));
	bool *found_sig = malloc(sizeof(bool));
	*(found_sig) = false;
	struct node *start = (struct node *)malloc(sizeof(struct node));
	start->next = NULL;
	start->prev = NULL;
	start->reposts = NULL;
	struct link_search *search_info = (struct link_search *)malloc(sizeof(struct link_search));
	search_info->path_idxs = NULL;
	search_info->n_steps = 0;
	search_info->queue_status = queue_status;
	search_info->found_sig = found_sig;
	search_info->users = users;
	search_info->index = targetA;
	search_info->target = targetB;
	start->path = search_info;
	
	enqueue(helper->work_queue, start, queue_status);
	
	struct threads **thread_info = helper->thread_info;
	for (int i = 0; i < helper->n_threads; i++) {
		thread_info[i]->sig = SIGLINK;
	}
	
	struct link_search *query_result = NULL;
	size_t *pathAtoB = NULL;
	size_t steps_AtoB = 0;
	while(1) {
		bool workdone = true;
		for (int i = 0; i < helper->n_threads; i++) {
			if (thread_info[i]->sig != 0) {
				workdone = false;
			}
			else {
				if (thread_info[i]->query_result != NULL) {
					query_result = (struct link_search *)thread_info[i]->query_result;
					if (pathAtoB == NULL) {
						pathAtoB = query_result->path_idxs;
						steps_AtoB = query_result-> n_steps;						
					}
					else {
						free(query_result->path_idxs);
					}
					free(query_result);
					thread_info[i]->query_result = NULL;
				}
			}
		}
		if (workdone) {
			break;
		}
	}
	
	free(queue_status);
	free(found_sig);
	clear_queue(helper->work_queue);
	
	//Find B to A
	bool *queue_status2 = calloc(count, sizeof(bool));
	bool *found_sig2 = malloc(sizeof(bool));
	*(found_sig2) = false;
	struct node *start2 = (struct node *)malloc(sizeof(struct node));
	start2->next = NULL;
	start2->prev = NULL;
	start2->reposts = NULL;
	struct link_search *search_info2 = (struct link_search *)malloc(sizeof(struct link_search));
	search_info2->path_idxs = NULL;
	search_info2->n_steps = 0;
	search_info2->queue_status = queue_status2;
	search_info2->found_sig = found_sig2;
	search_info2->users = users;
	search_info2->index = targetB;
	search_info2->target = targetA;
	start2->path = search_info2;
	
	enqueue(helper->work_queue, start2, queue_status2);
	
	for (int i = 0; i < helper->n_threads; i++) {
		if (thread_info[i]->query_result != NULL) {
			free(thread_info[i]->query_result);
		}
		thread_info[i]->sig = SIGLINK;
	}
	
	struct link_search *query_result2 = NULL;
	while(1) {
		bool workdone = true;
		for (int i = 0; i < helper->n_threads; i++) {
			if (thread_info[i]->sig != 0) {
				workdone = false;
			}
			else {
				if (thread_info[i]->query_result != NULL) {
					query_result2 = (struct link_search *)thread_info[i]->query_result;
					thread_info[i]->query_result = NULL;
				}
			}
		}
		if (workdone) {
			break;
		}
	}
	
	free(queue_status2);
	free(found_sig2);
	clear_queue(helper->work_queue);
	
	size_t *pathBtoA = NULL;
	size_t steps_BtoA = 0;
	if (query_result2 != NULL) {
		pathBtoA = query_result2->path_idxs;
		steps_BtoA = query_result2->n_steps;
		free(query_result2);
	}
	
	size_t *shortest_path = NULL;
	size_t n_steps = 0;
	size_t *toFree = NULL;
	
	if (pathAtoB == NULL && pathBtoA == NULL) {
		returnval->elements = NULL;
		returnval->n_elements = 0;
		return returnval;
	}
	
	if (pathAtoB == NULL || steps_AtoB > steps_BtoA) {
		shortest_path = pathBtoA;
		toFree = pathAtoB;
		n_steps = steps_BtoA;
	}
	else if (pathBtoA == NULL || steps_BtoA >= steps_AtoB) {
		shortest_path = pathAtoB;
		toFree = pathBtoA;
		n_steps = steps_AtoB;
	}
	
	struct user **elements = (struct user **)malloc(n_steps * sizeof(struct user *));
	for (int i = 0; i < n_steps; i++) {
		elements[i] = &users[shortest_path[i]];
	}
	free(shortest_path);
	if (toFree != NULL) {
		free(toFree);
	}
	
	returnval->elements = (void **)elements;
	returnval->n_elements = n_steps;
	return returnval;
}

result* find_bots(user* users, size_t user_count, post* posts, size_t post_count, criteria* crit, query_helper* helper) {
	struct result *returnval = (struct result *)malloc(sizeof(struct result));
	
	if (users == NULL || user_count == 0 || posts == NULL || post_count == 0 || crit == NULL) {
		returnval->elements = NULL;
		returnval->n_elements = 0;
		return returnval;
	}
	
	bool invalid_crit = false;
	if (crit->oc_threshold < 0 || crit->oc_threshold > 1) {
		invalid_crit = true;
	}
	else if (crit->acc_rep_threshold < 0 || crit->acc_rep_threshold > 1) {
		invalid_crit = true;
	}
	else if (crit->bot_net_threshold < 0 || crit->bot_net_threshold > 1) {
		invalid_crit = true;
	}
	
	if (invalid_crit) {
		returnval->elements = NULL;
		returnval->n_elements = 0;
		return returnval;
	}
	
	bool *isRoot = (bool *)calloc(post_count, sizeof(bool));
	for (int i = 0; i < post_count; i++) {
		struct post cur_post = posts[i];
		size_t n_reposted = cur_post.n_reposted;
		if (n_reposted > 0) {
			size_t *reposted_idxs = cur_post.reposted_idxs;
			for (int n = 0; n < n_reposted; n++) {
				isRoot[reposted_idxs[n]] = true;
			}
		}
	}
	
	size_t *work = divideWork(user_count, helper->n_threads);
	initiate_bot_search(work, isRoot, crit, users, helper);
	free(isRoot);
	
	struct user **elements = (struct user **)malloc(user_count * sizeof(struct user *));
	size_t n_elements = 0;
	struct threads **thread_info = helper->thread_info;
	bool *isBot = calloc(user_count, sizeof(bool));
	for (int i = 0; i < helper->n_threads; i++) {
		struct bot_info *query_result = (struct bot_info *)thread_info[i]->query_result;
		thread_info[i]->query_result = NULL;
		if (query_result != NULL) {
			size_t *bot_idxs = query_result->bot_idxs;
			for (int n = 0; n < query_result->n_bots; n++) {
				isBot[bot_idxs[n]] = true;
				elements[n_elements+n] = &(users[bot_idxs[n]]);
			}
			n_elements += query_result->n_bots;
			free(bot_idxs);
			free(query_result);
		}
	}
	
	while(1) {
		initiate_discrete_bot_search(work, isBot, crit->bot_net_threshold, users, helper);
		bool noNewResult = true;
		for (int i = 0; i < helper->n_threads; i++) {
			struct bot_info *query_result = (struct bot_info *)thread_info[i]->query_result;
			if (query_result != NULL) {
				noNewResult = false;
				size_t *bot_idxs = query_result->bot_idxs;
				for (int n = 0; n < query_result->n_bots; n++) {
					elements[n_elements+n] = &(users[bot_idxs[n]]);
				}
				n_elements += query_result->n_bots;
				free(bot_idxs);
				free(query_result);
				thread_info[i]->query_result = NULL;
			}
		}
		
		if (noNewResult) {
			for (int i = 0; i < helper->n_threads; i++) {
				thread_info[i]->sig = SIGSTOPBOTSEARCH;
			}
			break;
		}
	}
	free(isBot);
	free(work);
	
	elements = (struct user **)realloc(elements, n_elements * sizeof(struct user *));
	returnval->elements = (void **)elements;
	returnval->n_elements = n_elements;
	return returnval;
}

void engine_cleanup(query_helper* helpers) {
	for (int i = 0; i < helpers->n_threads; i++) {
		((helpers->thread_info)[i])->sig = SIGQUIT;
		if ((helpers->thread_info)[i]->search_info != NULL) {
			free(helpers->thread_info[i]->search_info);
		}
		if((helpers->thread_info)[i]->query_result != NULL) {
			free((helpers->thread_info[i])->query_result);
		}
		pthread_join(helpers->tids[i], NULL);
	}

	sem_destroy(helpers->work_status);
	free(helpers->work_status);
	free(helpers->thread_info);
	free(helpers->tids);
	clear_queue(helpers->work_queue);
	free(helpers->work_queue->head);
	free(helpers->work_queue->dummy);
	sem_destroy(helpers->work_queue->num_in_queue);
	free(helpers->work_queue->num_in_queue);
	free(helpers->work_queue);

	pthread_mutex_destroy(&enqueue_lock);
	pthread_mutex_destroy(&dequeue_lock);
	pthread_mutex_destroy(&add_elements);

	free(helpers);
}
