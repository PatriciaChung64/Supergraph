#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "queue_threads.h"
#include "supergraph.h"

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

#ifndef SUPERGRAPH_WORKER_H
#define SUPERGRAPH_WORKER_H

pthread_mutex_t enqueue_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t dequeue_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t add_elements = PTHREAD_MUTEX_INITIALIZER;

struct bot_info{
	size_t *bot_idxs;
	size_t n_bots;
};

struct threads {
	struct queue *work_queue;
	sem_t *work_status; 

	bool *queue_status;

	void *search_info;
	int sig;

	void *query_result;
};

void search_target_worker(struct threads *info);
void find_reposts_worker(struct threads *info);
void find_original_worker(struct threads *info);
void find_shortest_path_worker(struct threads *info);
void find_bots_worker(struct threads *info);
void find_discrete_bots_worker(struct threads *info);

void *worker(void *args) {
	struct threads *info = (struct threads *)args;
	while (1) {
		if (info->sig == SIGSEARCH_P || info->sig == SIGSEARCH_U) {
			search_target_worker(info);
			sem_wait(info->work_status);
		}

		else if (info->sig == SIGREPOST) {
			find_reposts_worker(info);
		}

		else if (info->sig == SIGLINK) {
			find_shortest_path_worker(info);
			info->sig = 0;
		}

		else if (info->sig == SIGORIGINAL) {
			find_original_worker(info);
			info->sig = 0;
			sem_wait(info->work_status);
		}

		else if (info->sig == SIGORIGINALFOUND) {
			// printf("Freeing search info\n");
			free(info->search_info);
			info->search_info = NULL;
			info->sig = 0;
		}

		else if (info->sig == SIGBOT) {
			find_bots_worker(info);
			info->sig = 0;
		}

		else if (info->sig == SIGDISCRETEBOT) {
			find_discrete_bots_worker(info);
			info->sig = 0;
		}
		
		else if (info->sig == SIGSTOPBOTSEARCH) {
			free(info->search_info);
			info->search_info = NULL;
			info->sig = 0;
		}

		else if (info->sig == SIGQUIT) {
			free(args);
			pthread_exit(NULL);
		}
	}
}

void search_target_worker(struct threads *info) {
	struct entry_search *search_info = (struct entry_search *)info->search_info;
	if (info->sig == SIGSEARCH_P) {
		struct post *posts = (struct post *)(search_info->elements);
		uint64_t *target = search_info->target;
		uint64_t target1 = target[0];
		for (int i = search_info->start; i < search_info->end; i++) {
			if (posts[i].pst_id == target1) {
				size_t *result = (size_t *)malloc(sizeof(size_t));
				*(result) = i;
				info->query_result = (void *)result;
			}
		}
		free(info->search_info);
		info->search_info = NULL;
		info->sig = 0;
	}
	else {
		size_t found = 0;
		size_t idx1 = 0;
		size_t idx2 = 0;
		uint64_t *target = search_info->target;
		uint64_t target1 = target[0];
		uint64_t target2 = target[1];
		struct user *users = (struct user *)(search_info->elements);
		for (int i = search_info->start; i < search_info->end; i++) {
			if (users[i].user_id == target1 || users[i].user_id == target2) {
				if (found == 0) {
					idx1 = i;
					found++;
				}
				else if (found == 1) {
					idx2 = i;
					found++;
				}
			}
		}
		free(info->search_info);
		info->search_info = NULL;
		info->sig = 0;
		
		if (found > 0) {
			size_t *query_result = (size_t *)malloc((found+1) * sizeof(size_t));
			query_result[0] = found;
			query_result[1] = idx1;
			if (found == 2) {
				query_result[2] = idx2;
			}
			info->query_result = (void *)query_result;
		}
	}

	return;
}

void find_reposts_worker(struct threads *info) {
	struct queue *work_queue = info->work_queue;
	int finished = 0;
	while (1) {
		sem_getvalue(info->work_status, &finished);
		if (getSize(info->work_queue) == 1 && finished == 1) {
			info->sig = 0;
			break;
		}

		struct node *node = NULL;
		pthread_mutex_lock(&dequeue_lock);
		if (getSize(info->work_queue) > 1) {
			node = dequeue(work_queue);
		}
		pthread_mutex_unlock(&dequeue_lock);

		if (node != NULL) {
			sem_post(info->work_status);

			struct post_search *search_info = node->reposts;

			pthread_mutex_lock(&add_elements);
			search_info->elements[*(search_info->n_elements)] = &((search_info->posts)[search_info->index]);
			*(search_info->n_elements) += 1;
			pthread_mutex_unlock(&add_elements);

			struct post cur_post = (search_info->posts)[search_info->index];
			bool *queue_status = info->queue_status;
			for (int i = 0; i < cur_post.n_reposted; i++) {
				if (queue_status[(cur_post.reposted_idxs)[i]] == 0) {
					struct node *toAdd = (struct node *)malloc(sizeof(struct node));
					toAdd->path = NULL;
					toAdd->prev = NULL;
					toAdd->next = NULL;

					struct post_search *new_info = (struct post_search *)malloc(sizeof(struct post_search));
					new_info->elements = search_info->elements;
					new_info->n_elements = search_info->n_elements;
					new_info->posts = search_info->posts;
					new_info->index = (cur_post.reposted_idxs)[i];
					toAdd->reposts = new_info;

					pthread_mutex_lock(&enqueue_lock);
					enqueue(info->work_queue, toAdd, queue_status);
					pthread_mutex_unlock(&enqueue_lock);
				}
			}
			free(node->reposts);
			free(node);

			sem_wait(info->work_status);
		}
	}
	return;
}

void find_original_worker(struct threads *info) {
	struct original_search *search_info = (struct original_search *)info->search_info;
	for (int i = search_info->start; i < search_info->end; i++) {
		if (i != *(search_info->target)) {
			int foundval = 0;
			sem_getvalue(search_info->found, &foundval);
			if (foundval == 2) {
				// printf("Already found an original, exiting\n");
				break;
			}
			else {
				// printf("Checking reposted list of post index %d\n", i);
				struct post cur_post = search_info->posts[i];
				for (int n = 0; n < cur_post.n_reposted; n++) {
					if (cur_post.reposted_idxs[n] == *(search_info->target)) {
						// printf("Found a new original, setting target to %d\n", i);
						sem_post(search_info->found);
						*(search_info->target) = i;
						foundval = 2;
						break;
					}
				}
				if (foundval == 2) {
					break;
				}
			}		  
		}
	}
}

void find_shortest_path_worker(struct threads *info) {
	struct queue *work_queue = info->work_queue;
	sem_t *work_status = info->work_status;
	while(1) {
		int workdone = 0;
		sem_getvalue(work_status, &workdone);
		if (getSize(work_queue) == 1 && workdone == 1) {
			break;
		}
		
		struct node *work = NULL;
		pthread_mutex_lock(&dequeue_lock);
		if(getSize(work_queue) > 1) {
			work = dequeue(work_queue);
			sem_post(work_status);
		}
		pthread_mutex_unlock(&dequeue_lock);
		
		if (work != NULL) {
			struct link_search *search_info = work->path;
			bool found_sig = *(search_info->found_sig);
			if (found_sig == true) {
				free(search_info->path_idxs);
				free(search_info);
				free(work);
			}
			else {
				size_t *path_idxs = search_info->path_idxs;
				size_t n_steps = search_info->n_steps;
				if (path_idxs == NULL) {
					path_idxs = (size_t *)malloc(sizeof(size_t));
				}
				else if (n_steps > 0) {
					path_idxs = (size_t *)realloc(path_idxs, (n_steps+1) * sizeof(size_t));
				}
				path_idxs[n_steps] = search_info->index;
				n_steps++;

				if (search_info->index == search_info->target) {
					struct link_search *search_result = (struct link_search *)malloc(sizeof(struct link_search));
					search_result->path_idxs = path_idxs;
					search_result->n_steps = n_steps;
					info->query_result = (void *)search_result;
					*(search_info->found_sig) = true;
				}

				else {
					struct user cur_user = (search_info->users)[search_info->index];
					size_t n_following = cur_user.n_following;
					bool *visit_status = search_info->queue_status;
					if (n_following > 0) {
						size_t *following_idxs = cur_user.following_idxs;
						for (int i = 0; i < n_following; i++) {
							if (visit_status[following_idxs[i]] != true) {
								struct node *toAdd = (struct node *)malloc(sizeof(struct node));
								toAdd->prev = NULL;
								toAdd->next = NULL;
								toAdd->reposts = NULL;

								struct link_search *new_info = (struct link_search *)malloc(sizeof(struct link_search));
								new_info->users = search_info->users;
								new_info->queue_status = visit_status;
								new_info->found_sig = search_info->found_sig;
								new_info->index = following_idxs[i];
								new_info->target = search_info->target;
								size_t *path_cpy = (size_t *)malloc(n_steps * sizeof(size_t));
								memcpy(path_cpy, path_idxs, n_steps * sizeof(size_t));
								new_info->path_idxs = path_cpy;
								new_info->n_steps = n_steps;
								
								toAdd->path = new_info;
								
								pthread_mutex_lock(&enqueue_lock);
								enqueue(work_queue, toAdd, visit_status);
								pthread_mutex_unlock(&enqueue_lock);

							}
						}
					}
					free(path_idxs);
				}
				
				free(search_info);
				free(work);
			}
			sem_wait(work_status);
		}
	}
	return;
}

void find_bots_worker(struct threads *info) {
	struct bot_search *search_info = (struct bot_search *)info->search_info;
	size_t start = search_info->start;
	size_t end = search_info->end;
	float oc_threshold = search_info->oc_threshold;
	float acc_rep_threshold = search_info->acc_rep_threshold;
	if (start != end) {
		size_t *bot_idxs = (size_t *)malloc((end-start+1) * sizeof(size_t));
		size_t bot_count = 0;
		struct user *users = search_info->users;
		bool *isRoot = search_info->isRoot;
		for (int i = start; i < end; i++) {
			struct user cur_user = users[i];
			size_t n_reposts = 0;
			size_t n_posts = cur_user.n_posts;
			size_t *post_idxs = cur_user.post_idxs;
			for (int n = 0; n < n_posts; n++) {
				if (isRoot[post_idxs[n]] == true) {
					n_reposts++;
				}
			}
			//calculate oc_threshold
			float reposts = (float)n_reposts;
			if ((float)reposts > ((float)oc_threshold * (float)n_posts)) {
				bot_idxs[bot_count] = i;
				bot_count++;
			}
			//calculate acc_rep_threshold
			else if ((float)cur_user.n_followers < ((float)acc_rep_threshold * (float)(cur_user.n_followers + cur_user.n_following))) {
				bot_idxs[bot_count] = i;
				bot_count++;
			}
		}
		if (bot_count > 0) {
			bot_idxs = (size_t *)realloc(bot_idxs, (bot_count) * sizeof(size_t));
			struct bot_info *query_result = (struct bot_info *)malloc(sizeof(struct bot_info));
			query_result->bot_idxs = bot_idxs;
			query_result->n_bots = bot_count;
			info->query_result = (void *)query_result;		
		}
		else {
			free(bot_idxs);
		}
	}
	free(search_info);
	info->search_info = NULL;
	return;
}

void find_discrete_bots_worker(struct threads *info) {
	struct discrete_bot_search *search_info = (struct discrete_bot_search *)info->search_info;
	bool *isBot = search_info->isBot;
	size_t start = search_info->start;
	size_t end = search_info->end;
	float bot_net_threshold = search_info->bot_net_threshold;
	if (start != end) {
		size_t *bot_idxs = (size_t *)malloc((end-start+1) * sizeof(size_t));
		size_t bot_count = 0;
		struct user *users = search_info->users;
		for (int i = start; i < end; i++) {
			if (isBot[i] != true) {
				struct user cur_user = users[i];
				size_t n_followers = cur_user.n_followers;
				size_t *follower_idxs = cur_user.follower_idxs;
				size_t bots_following = 0;
				for (int n = 0; n < n_followers; n++) {
					if (isBot[follower_idxs[n]] == true) {
						bots_following++;
					}
				}
				
				if ((float)bots_following > (float)bot_net_threshold * (float)(n_followers)) {
					isBot[i] = true;
					bot_idxs[bot_count] = i;
					bot_count++;
				}
			}
		}
		
		if (bot_count != 0) {
			bot_idxs = (size_t *)realloc(bot_idxs, bot_count * sizeof(size_t));
			struct bot_info *query_result = (struct bot_info *)malloc(sizeof(struct bot_info));
			query_result->bot_idxs = bot_idxs;
			query_result->n_bots = bot_count;
			info->query_result = (void *)query_result;
		}
		else {
			free(bot_idxs);
		}
	}
	return;
}

#endif
