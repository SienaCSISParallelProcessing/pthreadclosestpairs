/*
  pthreads implementation to find the closest pairs of waypoints
  in each of a set of METAL TMG graph files.

  When launched, the original thread creates p worker threads to
  parallelize the outer loop of the closest pairs computation for
  each graph (one at a time).

  The tasks to complete are to find the closest pair of points in
  METAL TMG files given as command-line parameters in argv[2] through
  argv[argc-1].

  The number of worker threads to create is passed as argv[1], and must 
  be a positive integer value.

  Jim Teresco, Fall 2021
  Siena College
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "timer.h"
#include "tmggraph.h"

/* struct for info to pass to the worker threads */
typedef struct tinfo {
  int thread_num;
  int num_threads;
  pthread_t threadid;
  pthread_mutex_t *mutex;
  tmg_graph *g;
  int *v1;
  int *v2;
  double *distance;
} tinfo;
  
/* worker thread function, which will complete all iterations of the
   outer loop such that the index % num_threads is this thread's id */
void *cp_worker(void *arg) {

  tinfo *ti = (tinfo *)arg;
  
  // variables for our local leaders
  int local_v1 = -1;
  int local_v2 = -1;
  double local_distance = 10000;    // larger than earth diameter
  
  int vert1, vert2;
  double this_dist;

  // compute the local leaders for our subset of the rows
  for (vert1 = ti->thread_num; vert1 < ti->g->num_vertices - 1;
       vert1+=ti->num_threads) {
    for (vert2 = vert1 + 1; vert2 < ti->g->num_vertices; vert2++) {
      this_dist = tmg_distance_latlng(&(ti->g->vertices[vert1]->w.coords),
				      &(ti->g->vertices[vert2]->w.coords));
      if (this_dist < local_distance) {
	local_distance = this_dist;
	local_v1 = vert1;
	local_v2 = vert2;
      }
    }
  }

  // contribute our local result to the overall result
  pthread_mutex_lock(ti->mutex);
  if (local_distance < *ti->distance) {
    *ti->distance = local_distance;
    *ti->v1 = local_v1;
    *ti->v2 = local_v2;
  }
  pthread_mutex_unlock(ti->mutex);
  
  return NULL;
}

/* closest pairs of vertices with pthreads */
void tmg_closest_pair_threads(int num_threads, tmg_graph *g,
			      int *v1, int *v2, double *distance) {

  pthread_mutex_t mutex;
  pthread_mutex_init(&mutex, NULL);
  
  // initialize this search
  *v1 = -1;
  *v2 = -1;
  *distance = 10000;  // larger than earth diameter
  
  
  // create thread info structs and launch the threads
  tinfo *thread_info = (tinfo *)malloc(num_threads*sizeof(tinfo));
  for (int thread_rank = 0; thread_rank < num_threads; thread_rank++) {
    thread_info[thread_rank].thread_num = thread_rank;
    thread_info[thread_rank].num_threads = num_threads;
    thread_info[thread_rank].g = g;
    thread_info[thread_rank].v1 = v1;
    thread_info[thread_rank].v2 = v2;
    thread_info[thread_rank].distance = distance;
    thread_info[thread_rank].mutex = &mutex;          // shared

    pthread_create(&(thread_info[thread_rank].threadid), NULL, cp_worker,
		   &(thread_info[thread_rank]));
  }

  // wait for threads to complete
  for (int thread_rank = 0; thread_rank < num_threads; thread_rank++) {
    pthread_join(thread_info[thread_rank].threadid, NULL);
  }

  pthread_mutex_destroy(&mutex);
  free(thread_info);
}

int main(int argc, char *argv[]) {
  
  int num_threads;
  
  // about how many distance calculations?
  long dcalcs = 0;

  int worker_rank;
  int num_tasks;

  int i;

  struct timeval start_time, stop_time;
  double active_time;

  // all parameters except argv[0] (program name) and argv[1] (number
  // of worker threads) will be filenames to load, so the number of
  // tasks is argc - 2
  num_tasks = argc - 2;
  
  if (argc < 3) {
    fprintf(stderr, "Usage: %s num_threads filenames\n", argv[0]);
    exit(1);
  }

  num_threads = strtol(argv[1], NULL, 10);
  
  if (num_threads < 1) {
    fprintf(stderr, "Must have at least 1 worker thread\n");
    exit(1);
  }

  printf("Have %d tasks to be done using %d worker threads\n", num_tasks,
	 num_threads);
  
  // start the timer
  gettimeofday(&start_time, NULL);
  
  printf("Will create %d threads to help complete %d jobs\n", num_threads, num_tasks);
  
  // go through the files
  for (int task_pos = 2; task_pos < argc; task_pos++) {
    gettimeofday(&stop_time, NULL);
    active_time = diffgettime(start_time, stop_time);
    printf("Starting task %d %s at elapsed time %.6f\n", (task_pos-1),
	   argv[task_pos], active_time);

    tmg_graph *g = tmg_load_graph(argv[task_pos]);
    if (g == NULL) {
      fprintf(stderr, "Could not create graph from file %s, SKIPPING\n",
	      argv[task_pos]);
      continue;
    }
    
    int v1, v2;
    double distance;
    
    // do it
    tmg_closest_pair_threads(num_threads, g, &v1, &v2, &distance);
    
    printf("%s closest pair #%d %s (%.6f,%.6f) and #%d %s (%.6f,%.6f) distance %.15f\n",
	   argv[task_pos], v1, g->vertices[v1]->w.label,
	   g->vertices[v1]->w.coords.lat, g->vertices[v1]->w.coords.lng,
	   v2, g->vertices[v2]->w.label,
	   g->vertices[v2]->w.coords.lat, g->vertices[v2]->w.coords.lng,
	   distance);
    
    tmg_graph_destroy(g);
  }
  
  // get main thread's elapsed time
  gettimeofday(&stop_time, NULL);
  active_time = diffgettime(start_time, stop_time);


  printf("Main thread was active for %.4f seconds\n", active_time);
  return 0;
}
