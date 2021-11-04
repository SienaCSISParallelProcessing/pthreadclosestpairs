/*
  Bag of Tasks pthreads implementation to find the closest pairs of waypoints
  in each of a set of METAL TMG graph files.

  When launched, the original thread creates p worker threads.

  The tasks to complete are to find the closest pair of points in
  METAL TMG files given as command-line parameters in argv[3] through
  argv[argc-1].

  The tasks are distributed in an order based on the string passed as
  argv[1], which is one of:

      "orig": the order that the files are presented on the command line
      "alpha": alphabetical order by filename
      "size": from largest to smallest number of points in the file
      "random": randomized order

  The number of worker threads to create is passed as argv[2], and must 
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

// struct to encapsulate info about the tasks in the bag
typedef struct cptask {
  int num_vertices;
  char *filename;
} cptask;

// struct to pass info to the threads through their one parameter
typedef struct tinfo {
  pthread_t threadid;
  int thread_num;
  cptask **tasks;
  int num_tasks;
  int *next_task;
  pthread_mutex_t *mutex;
  double thread_elapsed_time;
  int jobs_done;
  long dcalcs;
} tinfo;

// helper function to read only up to the number of vertices from a
// TMG file and return that number
int read_tmg_vertex_count(char *filename) {

  FILE *fp = fopen(filename, "r");
  if (!fp) {
    fprintf(stderr, "Cannot open file %s for reading.\n", filename);
    exit(1);
  }

  // read over first line
  char temp[100];
  fscanf(fp, "%s %s %s", temp, temp, temp);
  
  // read number of vertices
  int nv;
  fscanf(fp, "%d", &nv);

  // that's all we need for now
  fclose(fp);

  return nv;
}

// the worker thread
void *worker(void *arg) {

  tinfo *ti = (tinfo *)arg;
  struct timeval start_time, stop_time;
  double active_time;

  // start the timer
  gettimeofday(&start_time, NULL);

  int my_task = -1;
  ti->jobs_done = 0;
  ti->dcalcs = 0L;
  
  while (1) {

    // grab a task from the bag
    pthread_mutex_lock(ti->mutex);
    my_task = *ti->next_task;
    *ti->next_task += 1;
    pthread_mutex_unlock(ti->mutex);
    if (my_task >= ti->num_tasks) break;

    // this thread can process this one
    printf("[%d] working on %s\n", ti->thread_num,
	   ti->tasks[my_task]->filename);
    tmg_graph *g = tmg_load_graph(ti->tasks[my_task]->filename);
    if (g == NULL) {
      fprintf(stderr, "Could not create graph from file %s\n",
	      ti->tasks[my_task]->filename);
      exit(1);
    }
    
    int v1, v2;
    double distance;
    
    // do it
    tmg_closest_pair(g, &v1, &v2, &distance);
    
    ti->jobs_done++;
    long job_calcs = g->num_vertices;
    job_calcs *= g->num_vertices;
    job_calcs /= 2;
    ti->dcalcs += job_calcs;
    
    printf("[%d] %s closest pair #%d %s (%.6f,%.6f) and #%d %s (%.6f,%.6f) distance %.15f\n",
	   ti->thread_num, ti->tasks[my_task]->filename, v1,
	   g->vertices[v1]->w.label,
	   g->vertices[v1]->w.coords.lat, g->vertices[v1]->w.coords.lng,
	   v2, g->vertices[v2]->w.label,
	   g->vertices[v2]->w.coords.lat, g->vertices[v2]->w.coords.lng,
	   distance);
    
    tmg_graph_destroy(g);
  }

  gettimeofday(&stop_time, NULL);

  ti->thread_elapsed_time = diffgettime(start_time, stop_time);

  printf("[%d] terminating\n", ti->thread_num);
  return NULL;
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

  // all parameters except argv[0] (program name), argv[1] (input
  // ordering), and argv[2] (number of worker threads) will be
  // filenames to load, so the number of tasks is argc - 3
  num_tasks = argc - 3;
  
  if (argc < 4) {
    fprintf(stderr, "Usage: %s orig|alpha|size|random num_threads filenames\n", argv[0]);
    exit(1);
  }

  // check for a valid ordering in argv[1];
  char *orderings[] = {
    "orig",
    "alpha",
    "size",
    "random"
  };
  int ordering = -1;
  for (i = 0; i < 4; i++) {
    if (strcmp(argv[1], orderings[i]) == 0) {
      ordering = i;
      break;
    }
  }
  if (ordering == -1) {
    fprintf(stderr, "Usage: %s orig|alpha|size|random filenames\n", argv[0]);
    exit(1);
  }      

  num_threads = strtol(argv[2], NULL, 10);
  
  if (num_threads < 1) {
    fprintf(stderr, "Must have at least 1 worker thread\n");
    exit(1);
  }

  printf("Have %d tasks to be done by %d worker threads\n", num_tasks,
	 num_threads);
  
  // start the timer
  gettimeofday(&start_time, NULL);
  
  // allocate and populate our "bag of tasks" array
  cptask **tasks = (cptask **)malloc(num_tasks*sizeof(cptask *));
  
  // add the first at pos 0, since we know there's at least one and
  // this will eliminate some special cases in our code below.
  tasks[0] = (cptask *)malloc(sizeof(cptask));
  tasks[0]->filename = argv[3];
  if (ordering == 2) {
    tasks[0]->num_vertices = read_tmg_vertex_count(argv[3]);
  }
  
  // get them all in
  for (i = 1; i < num_tasks; i++) {
    cptask *taski = (cptask *)malloc(sizeof(cptask));
    taski->filename = argv[i+3];
    int pos = i;
    int insertat;
    switch (ordering) {
      
    case 0:
      // original ordering as specified by argv
      tasks[i] = taski;
      break;
      
      
    case 1:
      // alphabetical order by filename
      while (pos > 0 && strcmp(taski->filename, tasks[pos-1]->filename) < 0) {
	tasks[pos] = tasks[pos-1];
	pos--;
      }
      tasks[pos] = taski;
      
      break;
      
    case 2:
      // order by size largest to smallest number of vertices
      taski->num_vertices = read_tmg_vertex_count(taski->filename);
      while (pos > 0 && taski->num_vertices >= tasks[pos-1]->num_vertices) {
	tasks[pos] = tasks[pos-1];
	pos--;
      }
      tasks[pos] = taski;
      
      break;
      
    case 3:
      // order randomly
      insertat = random()%(pos+1);
      while (pos > insertat) {
	tasks[pos] = tasks[pos-1];
	pos--;
      }
      tasks[pos] = taski;
      break;
    }
  }

  printf("Creating %d threads to complete %d jobs\n", num_threads, num_tasks);
  
  // what's the next task available in the bag of tasks (index into array)
  int next_task = 0;
  // mutex to protect it
  pthread_mutex_t mutex;
  pthread_mutex_init(&mutex, NULL);
  
  // construct the array of tinfo structs that will be passed to our
  // worker threads, and create our threads
  tinfo *thread_info = (tinfo *)malloc(num_threads*sizeof(tinfo));
  for (int thread_rank = 0; thread_rank < num_threads; thread_rank++) {
    thread_info[thread_rank].thread_num = thread_rank;
    thread_info[thread_rank].tasks = tasks;
    thread_info[thread_rank].num_tasks = num_tasks;
    thread_info[thread_rank].next_task = &next_task;  // shared
    thread_info[thread_rank].mutex = &mutex;          // shared

    pthread_create(&(thread_info[thread_rank].threadid), NULL, worker,
		   &(thread_info[thread_rank]));
  }

  // threads compute here

  // wait for each to finish up
  for (int thread_rank = 0; thread_rank < num_threads; thread_rank++) {
    pthread_join(thread_info[thread_rank].threadid, NULL);
  }

  // get main thread's elapsed time
  gettimeofday(&stop_time, NULL);
  active_time = diffgettime(start_time, stop_time);

  // clean up 
  pthread_mutex_destroy(&mutex);

  // compute some stats
  int minjobs = thread_info[0].jobs_done;
  int maxjobs = thread_info[0].jobs_done;
  double avgjobs = 1.0*num_tasks/num_threads;
  long mincalcs = thread_info[0].dcalcs;
  long maxcalcs = thread_info[0].dcalcs;
  long totalcalcs = thread_info[0].dcalcs;
  double mintime = thread_info[0].thread_elapsed_time;
  double maxtime = thread_info[0].thread_elapsed_time;
  for (worker_rank = 1; worker_rank < num_threads; worker_rank++) {
    if (thread_info[worker_rank].jobs_done < minjobs)
      minjobs = thread_info[worker_rank].jobs_done;
    if (thread_info[worker_rank].jobs_done > maxjobs)
      maxjobs = thread_info[worker_rank].jobs_done;
    if (thread_info[worker_rank].dcalcs < mincalcs)
      mincalcs = thread_info[worker_rank].dcalcs;
    if (thread_info[worker_rank].dcalcs > maxcalcs)
      maxcalcs = thread_info[worker_rank].dcalcs;
    totalcalcs += thread_info[worker_rank].dcalcs;
    if (thread_info[worker_rank].thread_elapsed_time < mintime)
      mintime = thread_info[worker_rank].thread_elapsed_time;
    if (thread_info[worker_rank].thread_elapsed_time > maxtime)
      maxtime = thread_info[worker_rank].thread_elapsed_time;
  }
  
  printf("Main thread was active for %.4f seconds\n", active_time);
  printf("%d workers processed %d jobs with about %ld distance calculations\n",
	 num_threads, num_tasks, totalcalcs);
  printf("Job balance: min %d, max %d, avg: %.2f\n", minjobs, maxjobs,
	 avgjobs);
  printf("Distance calculation balance: min %ld, max %ld, avg: %.2f\n",
	 mincalcs, maxcalcs, ((1.0*totalcalcs)/num_threads));
  printf("Active time balance: min %.4f, max %.4f\n", mintime, maxtime);
  for (worker_rank = 0; worker_rank < num_threads; worker_rank++) {
    printf("%d: %d job%s, %ld distance calculations, difference from avg: %.2f, time %.4f\n",
	   worker_rank, thread_info[worker_rank].jobs_done,
	   (thread_info[worker_rank].jobs_done == 1 ? "" : "s"),
	   thread_info[worker_rank].dcalcs,
	   (thread_info[worker_rank].dcalcs-((1.0*totalcalcs)/num_threads)),
	   thread_info[worker_rank].thread_elapsed_time);
  }

  for (i = 0; i < num_tasks; i++) {
    free(tasks[i]);
  }
  free(tasks);
  free(thread_info);
  return 0;
}
