/* (c) 2010,2011 SKA SA */
/* Released under the GNU GPLv3 - see COPYING */

/* Slightly higher level dispatch routines, handles callback registration
 * and dispatch. Wraps a fair amount of line handling stuff
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <limits.h>
#include <sysexits.h>

#include <sys/time.h>

#include "katpriv.h"
#include "katcl.h"
#include "katcp.h"

#define KATCP_READY_MAGIC 0x7d68352c

int setenv_cmd_katcp(struct katcp_dispatch *d, int argc);
int chdir_cmd_katcp(struct katcp_dispatch *d, int argc);
int forget_cmd_katcp(struct katcp_dispatch *d, int argc);
int help_cmd_katcp(struct katcp_dispatch *d, int argc);
int halt_cmd_katcp(struct katcp_dispatch *d, int argc);
int restart_cmd_katcp(struct katcp_dispatch *d, int argc);
int log_level_cmd_katcp(struct katcp_dispatch *d, int argc);
int log_record_cmd_katcp(struct katcp_dispatch *d, int argc);
int watchdog_cmd_katcp(struct katcp_dispatch *d, int argc);

/************ paranoia checks ***********************************/

#ifdef DEBUG
static void sane_katcp(struct katcp_dispatch *d)
{
  if(d == NULL){
    fprintf(stderr, "sane: invalid handle\n");
    abort();
  }
  if(d->d_line == NULL){
    fprintf(stderr, "sane: invalid line handle\n");
    abort();
  }
#if 0
  if(d->d_ready != KATCP_READY_MAGIC){
    fprintf(stderr, "sane: not called from within dispatch\n");
    abort();
  }
#endif
}
#else
#define sane_katcp(d)
#endif

/******************************************************************/

static struct katcp_dispatch *setup_internal_katcp(int fd)
{
  struct katcp_dispatch *d;

  d = malloc(sizeof(struct katcp_dispatch));
  if(d == NULL){
    return NULL;
  }

  d->d_line = create_katcl(fd);

  d->d_current = NULL;
  d->d_ready = 0;

  d->d_run = 1;
  d->d_exit = KATCP_EXIT_ABORT;
  d->d_pause = 0;

  d->d_level = KATCP_LEVEL_INFO;

#if 0
  d->d_multi = NULL;
#endif

  d->d_shared = NULL;

  d->d_nonsense = NULL;
  d->d_size = 0;

  d->d_notices = NULL;
  d->d_count = 0;

  d->d_end = NULL;

  d->d_clone = (-1);

  d->d_name[0] = '\0';

  if(d->d_line == NULL){
    shutdown_katcp(d);
    return NULL;
  }

  return d;
}

/******************************************************************/

struct katcp_dispatch *setup_katcp(int fd)
{
  struct katcp_dispatch *d;

  d = setup_internal_katcp(fd);
  if(d == NULL){
    return NULL;
  }

  if(startup_shared_katcp(d) < 0){
    shutdown_katcp(d);
    return NULL;
  }

  register_flag_mode_katcp(d, "?setenv",  "sets/clears an enviroment variable (?setenv [label [value]]", &setenv_cmd_katcp, KATCP_CMD_HIDDEN, 0);
  register_flag_mode_katcp(d, "?chdir",   "change directory (?chdir directory)", &chdir_cmd_katcp, KATCP_CMD_HIDDEN, 0);
  register_flag_mode_katcp(d, "?forget",  "deregister a command (?forget command)", &forget_cmd_katcp, KATCP_CMD_HIDDEN, 0);

  register_katcp(d, "?halt",              "shuts the system down (?halt)", &halt_cmd_katcp);
  register_katcp(d, "?restart",           "restarts the system (?restart)", &restart_cmd_katcp);
  register_katcp(d, "?help",              "displays this help (?help [command])", &help_cmd_katcp);
  register_katcp(d, "?log-level",         "sets the minimum reported log priority (?log-level [priority])", &log_level_cmd_katcp);
  register_katcp(d, "?log-record",        "generate a log entry (?log-record [priority] message)", &log_record_cmd_katcp);
  register_katcp(d, "?watchdog",          "pings the system (?watchdog)", &watchdog_cmd_katcp);

  register_katcp(d, "?sensor-list",       "lists available sensors (?sensor-list [sensor])", &sensor_list_cmd_katcp);
  register_katcp(d, "?sensor-sampling",   "configure sensor (?sensor-sampling sensor [strategy [parameter]])", &sensor_sampling_cmd_katcp);
  register_katcp(d, "?sensor-value",      "query a sensor (?sensor-value sensor)", &sensor_value_cmd_katcp);


  return d;
}

struct katcp_dispatch *startup_katcp()
{
  return setup_katcp(-1);
}

#if 0
struct katcp_dispatch *setup_version_katcp(int fd, char *subsystem, int major, int minor)
{
  struct katcp_dispatch *d;

  d = setup_katcp(fd);
  if(d == NULL){
    return NULL;
  }

  if(version_katcp(d, subsystem, major, minor) < 0){
    shutdown_katcp(d);
    return NULL;
  }

  return d;
}

struct katcp_dispatch *startup_version_katcp(char *subsystem, int major, int minor)
{
  return setup_version_katcp(-1, subsystem, major, minor);
}
#endif

/******************/

struct katcp_dispatch *clone_katcp(struct katcp_dispatch *cd)
{
  struct katcp_dispatch *d;

  d = setup_internal_katcp(-1);
  if(d == NULL){
    return NULL;
  }

  d->d_level = cd->d_level;
  /* d_ready */
  /* d_run */
  /* d_exit */
  /* d_pause */
  /* d_line */

  if(link_shared_katcp(d, cd) < 0){
    shutdown_katcp(d);
    return NULL;
  }

  /* sensors are allocated on demand */

  return d;
}

/***********************************************************************/

void shutdown_cmd_katcp(struct katcp_cmd *c)
{
  if(c){
    if(c->c_name){
      free(c->c_name);
      c->c_name = NULL;
    }

    if(c->c_help){
      free(c->c_help);
      c->c_help = NULL;
    }

    free(c);
  }
}

void shutdown_katcp(struct katcp_dispatch *d)
{
  if(d == NULL){
    return;
  }

  destroy_nonsensors_katcp(d);

  disown_notices_katcp(d);

  shutdown_shared_katcp(d);

  if(d->d_line){
    destroy_katcl(d->d_line, 1);
    d->d_line = NULL;
  }

  d->d_current = NULL;

  free(d);
}

/********************/

int name_katcp(struct katcp_dispatch *d, char *fmt, ...)
{
  int result;
  va_list args;

  va_start(args, fmt);
  result = vsnprintf(d->d_name, KATCP_NAME_LENGTH - 1, fmt, args);
  va_end(args);

  if((result < 0) || (result >= KATCP_NAME_LENGTH)){
    d->d_name[0] = '\0';
    return -1;
  }

  d->d_name[result] = '\0';

  return 0;
}

int version_katcp(struct katcp_dispatch *d, char *subsystem, int major, int minor)
{
  return mode_version_katcp(d, 0, subsystem, major, minor);
}

int build_katcp(struct katcp_dispatch *d, char *build)
{
  return add_build_katcp(d, build);
}

int del_build_katcp(struct katcp_dispatch *d, int index)
{
  struct katcp_shared *s;

  if(d == NULL){
    return -1;
  }
  if(index < 0){
    return -1;
  }

  s = d->d_shared;
  if(s == NULL){
    return -1;
  }
  if(index >= s->s_build_items){
    return -1;
  }

  free(s->s_build_state[index]);
  s->s_build_state[index] = NULL;

  return 0;
}

int add_build_katcp(struct katcp_dispatch *d, char *build)
{
  struct katcp_shared *s;
  char **ptr;
  int pos;

  if(d == NULL){
    return -1;
  }

  s = d->d_shared;
  if(s == NULL){
    return -1;
  }

  for(pos = 0; (pos < s->s_build_items) && (s->s_build_state[pos] != NULL); pos++);

  if(pos >= s->s_build_items){
    ptr = realloc(s->s_build_state, sizeof(char *) * (s->s_build_items + 1));
    if(ptr == NULL){
      return -1;
    }
    s->s_build_state = ptr;
    s->s_build_state[s->s_build_items] = NULL;
    s->s_build_items++;
  }

  s->s_build_state[pos] = strdup(build);
  if(s->s_build_state[pos] == NULL){
    return -1;
  }

  return pos;
}

/***************************************************/

void set_state_katcp(struct katcp_dispatch *d, void *p)
{
  store_clear_mode_katcp(d, 0, p, NULL);
}

void set_clear_state_katcp(struct katcp_dispatch *d, void *p, void (*clear)(struct katcp_dispatch *d, unsigned int mode))
{
  store_clear_mode_katcp(d, 0, p, clear);
}

void *get_state_katcp(struct katcp_dispatch *d)
{
  return get_mode_katcp(d, 0);
}

/***************************************************/

/***************************************************/

#if 0
void set_multi_katcp(struct katcp_dispatch *d, void *p)
{
  if(d){
    d->d_multi = p;
  }
}

void *get_multi_katcp(struct katcp_dispatch *d)
{
  return d ? d->d_multi : NULL;
}
#endif

/********************/

void on_disconnect_katcp(struct katcp_dispatch *d, char *fmt, ...)
{
  va_list args;

#ifdef DEBUG
  fprintf(stderr, "disconnect: run=%d\n", d->d_run);
#endif

#ifdef PARANOID
  if(d->d_run > 0){
    fprintf(stderr, "disconnect: not yet terminated\n");
    abort();
  }
#endif

  if(exiting_katcp(d)){ /* ensures that we only run once */

    append_string_katcp(d, KATCP_FLAG_FIRST, "#disconnect");

#ifdef DEBUG
    fprintf(stderr, "disconnect: ending d=%p fd=%d\n", d, fileno_katcp(d));
#endif

    if(fmt){
      va_start(args, fmt);
      append_vargs_katcp(d, KATCP_FLAG_LAST, fmt, args);
      va_end(args);
    } else {
      switch(d->d_exit){
        case KATCP_EXIT_HALT :
          append_string_katcp(d, KATCP_FLAG_LAST, "system halting");
          break;
        case KATCP_EXIT_RESTART :
          append_string_katcp(d, KATCP_FLAG_LAST, "system restarting");
          break;

        case KATCP_EXIT_QUIT :
          /* won't say anything about a quit, the user has requested it */
          break;

          /* case KATCP_EXIT_ABORT : */
        default :
          append_string_katcp(d, KATCP_FLAG_LAST, "system aborting");
          break;
      }
    }
  }
}

void on_connect_katcp(struct katcp_dispatch *d)
{
  /* prints initial inform messages, could possibly be made dynamic */
  struct katcp_shared *s;
  struct katcp_entry *e;
  int i;

  if(d == NULL){
    return;
  }

  s = d->d_shared;
  if(s == NULL){
    return;
  }

  print_versions_katcp(d, 1);

  e = &(s->s_vector[s->s_mode]);

#if 0
  if(e->e_name || e->e_version){
    append_string_katcp(d, KATCP_FLAG_FIRST, "#version");
    append_args_katcp(d, KATCP_FLAG_LAST, "%s-%d.%d", e->e_version ? e->e_version : e->e_name, e->e_major, e->e_minor);
  }
#endif
  if(s->s_build_state){
    append_string_katcp(d, KATCP_FLAG_FIRST, "#build-state");

    for(i = 0; i < (s->s_build_items - 1); i++){
      append_string_katcp(d, 0, s->s_build_state[i]);
    }
    append_string_katcp(d, KATCP_FLAG_LAST, s->s_build_state[s->s_build_items - 1]);
  }
}

#if 0
pid_t spawn_child_katcp(struct katcp_dispatch *d, char *name, int (*run)(void *data), void *data, void (*call)(struct katcp_dispatch *d, int status))
{
  pid_t pid;
  int result;

  pid = fork();
  if(pid < 0){
#ifdef DEBUG
    fprintf(stderr, "start child: unable to fork: %s\n", strerror(errno));
#endif
    return -1;
  }

  if(pid > 0){
    /* TODO: close as many file descriptors as possible */
    if(watch_shared_katcp(d, name, pid, call) < 0){
      return -1;
    }
    return pid;
  }

#ifdef DEBUG  
  fprintf(stderr, "start child: running as child process\n");
#endif

  result = (*run)(data);

  if(result < 0){
    exit(EX_UNAVAILABLE);
  } else {
    exit(result & 0xff);
  }

  /* NOT REACHED */
  return EX_OK;
}
#endif

/* Need a call back for a sensor - unclear how to do this, as
 * sensors can happen over intervals, when values change, etc 
 */

int deregister_command_katcp(struct katcp_dispatch *d, char *match)
{
  struct katcp_cmd *c, *prv, *nxt;
  struct katcp_shared *s;
  char *ptr;
  int len;

  if((d == NULL) || (d->d_shared == NULL)){
    return -1;
  }

  s = d->d_shared;

  if(match == NULL){
    return -1;
  }

  switch(match[0]){
    case '\0' :
      log_message_katcp(d, KATCP_LEVEL_WARN, NULL, "refusing to deregister empty command");
      return -1;

    case KATCP_REQUEST :
    case KATCP_REPLY :
    case KATCP_INFORM :
      ptr = match;
      break;

    default :
      len = strlen(match) + 2;
      ptr = malloc(len);
      if(ptr == NULL){
        log_message_katcp(d, KATCP_LEVEL_FATAL, NULL, "unable to allocate %d bytes", len);
        return -1;
      }
      ptr[0] = KATCP_REQUEST;
      strcpy(ptr + 1, match);
      break;
  }

  prv = NULL;
  c = s->s_commands;

  /* WARNING: if we add state to a _cmd function, deletion may have troublesome side effects */

  while(c){
    nxt = c->c_next;
    if((c->c_name != NULL) && (strcmp(c->c_name, ptr) == 0)){
      if(prv){
        prv->c_next = nxt;
      } else {
        s->s_commands = nxt;
      }
      shutdown_cmd_katcp(c);
      if(ptr != match){
        free(ptr);
      }
      return 0;
    } else {
      prv = c;
    }
    c = nxt;
  }

#ifdef REPORT
  fprintf(stderr, "no match found for %s while deregistering command\n", match);
#endif
  log_message_katcp(d, KATCP_LEVEL_INFO, NULL, "no match found for %s", ptr);

  if(ptr != match){
    free(ptr);
  }

  return -1;
}

int register_flag_mode_katcp(struct katcp_dispatch *d, char *match, char *help, int (*call)(struct katcp_dispatch *d, int argc), int flags, int mode)
{
  struct katcp_cmd *c, *nxt;
  struct katcp_shared *s;
  
  if((d == NULL) || (d->d_shared == NULL)){
    return -1;
  }

  s = d->d_shared;

  if(mode >= s->s_size){
#ifdef REPORT
    fprintf(stderr, "register: registering command for oversized mode number %u\n", mode);
#endif
    log_message_katcp(d, KATCP_LEVEL_DEBUG, NULL, "mode number %d is not within range", mode);
    return -1;
  }

  c = malloc(sizeof(struct katcp_cmd));
  if(c == NULL){
    return -1;
  }

  c->c_name = NULL;
  c->c_help = NULL;
  c->c_call = NULL;
  c->c_next = NULL;
  c->c_mode = 0;
  c->c_flags = KATCP_CMD_HIDDEN;

  if(match == NULL){
    flags |= KATCP_CMD_WILDCARD;
  } else {
    switch(match[0]){
      case '\0' :
        break;
      case KATCP_REQUEST :
      case KATCP_REPLY   :
      case KATCP_INFORM   :
        c->c_name = strdup(match);
        break;
      default :
        c->c_name = malloc(strlen(match + 2));
        if(c->c_name){
          c->c_name[0] = KATCP_REQUEST;
          strcpy(c->c_name + 1, match);
        }
        break;
    }
    if(c->c_name == NULL){
      shutdown_cmd_katcp(c);
      return -1;
    }
  }

  c->c_help = strdup(help);
  if(c->c_help == NULL){
    shutdown_cmd_katcp(c);
    return -1;
  }

  c->c_call = call;

  c->c_mode = mode;
  c->c_flags = flags;


  if((flags & KATCP_CMD_WILDCARD) && s->s_commands){
#ifdef DEBUG
    fprintf(stderr, "register: appending wildcard command for mode %d\n", c->c_mode);
#endif
    nxt = s->s_commands;
    while(nxt->c_next){
      nxt = nxt->c_next;
    }

    nxt->c_next = c;
    c->c_next = NULL;

  } else {
#ifdef DEBUG
    fprintf(stderr, "register: prepended command %s for mode %d\n", c->c_name, c->c_mode);
#endif
    c->c_next = s->s_commands;
    s->s_commands = c;
  }

  return 0;
}

int register_mode_katcp(struct katcp_dispatch *d, char *match, char *help, int (*call)(struct katcp_dispatch *d, int argc), int mode)
{
  return register_flag_mode_katcp(d, match, help, call, 0, mode);
}

int register_katcp(struct katcp_dispatch *d, char *match, char *help, int (*call)(struct katcp_dispatch *d, int argc))
{
  return register_flag_mode_katcp(d, match, help, call, 0, 0);
}

int continue_katcp(struct katcp_dispatch *d, int when, int (*call)(struct katcp_dispatch *d, int argc))
{
  /* calling this function outside a registered callback is likely to cause grief */
  sane_katcp(d);

  d->d_current = call;

  return 0;
}

int lookup_katcp(struct katcp_dispatch *d)
{
  char *s;
  int r;
  struct katcp_cmd *search;

#ifdef REPORT
  if((d == NULL) || (d->d_shared == NULL)){
    fprintf(stderr, "lookup: state is null\n");
    return -1;
  }
#endif

  if(d->d_current){
    return 1; /* still busy from last time */
  }

  if(d->d_pause){
    return 0;
  }

  r = have_katcl(d->d_line);
  if(r < 0){
    return -1;
  }
  
  if(r == 0){
    return 0; /* nothing to be looked up */
  }

  /* now (r  > 0) */

  s = arg_string_katcl(d->d_line, 0);

  for(search = d->d_shared->s_commands; search; search = search->c_next){
#ifdef DEBUG
    fprintf(stderr, "dispatch: checking %s against %s\n", s, search->c_name);
#endif
    if(((search->c_mode == 0) || (search->c_mode == d->d_shared->s_mode)) && ((search->c_flags & KATCP_CMD_WILDCARD) || (!strcmp(search->c_name, s)))){
#ifdef DEBUG
      fprintf(stderr, "dispatch: found match for <%s>\n", s);
#endif
      d->d_current = search->c_call;
      return 1; /* found */
    }
  }
  
  return 1; /* not found, d->d_current == NULL */
}

int call_katcp(struct katcp_dispatch *d)
{
  int r, n;
  char *s;

#ifdef REPORT
  if(d == NULL){
    fprintf(stderr, "call: null dispatch argument\n");
    return -1;
  }
#endif

  d->d_ready = KATCP_READY_MAGIC;

  s = arg_string_katcl(d->d_line, 0);
  n = arg_count_katcl(d->d_line);
  r = KATCP_RESULT_FAIL;

  if(d->d_current){
    r = (*(d->d_current))(d, n);

#ifdef DEBUG
    fprintf(stderr, "call: dispatch function returned %d\n", r);
#endif

    if(r <= 0){
      extra_response_katcp(d, r, NULL);
    }
    if(r != KATCP_RESULT_YIELD){
      d->d_current = NULL;
    }
    if(r == KATCP_RESULT_PAUSE){
      if(d->d_count <= 0){
        log_message_katcp(d, KATCP_LEVEL_DEBUG, NULL, "request %s is pausing task %p without having notices pending", s, d);
      }
      d->d_pause = 1;
    }
  } else { /* force a fail for unknown commands */
#ifdef REPORT
    fprintf(stderr, "call: no dispatch function for %s\n", s);
#endif
    switch(s[0]){
      case KATCP_REQUEST :
        if(s[1] != '\0'){
          extra_response_katcp(d, KATCP_RESULT_INVALID, "unknown command");
#if 0
          send_katcp(d, 
              KATCP_FLAG_FIRST | KATCP_FLAG_MORE | KATCP_FLAG_STRING, "!", 
              KATCP_FLAG_STRING, s + 1, 
              KATCP_FLAG_STRING, KATCP_INVALID, 
              KATCP_FLAG_LAST | KATCP_FLAG_STRING, "unknown command");
#endif
        }
        break;
      case KATCP_REPLY :
        if(s[1] != '\0'){
          log_message_katcp(d, KATCP_LEVEL_ERROR, NULL, "unexpected reply %s", s + 1);
        }
        break;
      default :
        break;
    }
  }

  d->d_ready = 0;

  return r;
}

int dispatch_katcp(struct katcp_dispatch *d)
{
  int r;

  if(d->d_pause > 0){
#if DEBUG > 1
    fprintf(stderr, "dispatch: deferring because paused\n");
#endif
    return 0;
  }

#if DEBUG > 1
    fprintf(stderr, "dispatch: looking up\n");
#endif

  while((r = lookup_katcp(d)) > 0){
#if DEBUG > 1
    fprintf(stderr, "dispatch: calling\n");
#endif
    call_katcp(d);
  }

#if DEBUG > 1
  fprintf(stderr, "dispatch: lookup returns %d\n", r);
#endif

  return r;
}

/************ line wrappers, could be made macros ***************/

int fileno_katcp(struct katcp_dispatch *d)
{
  if(d && d->d_line){
    return fileno_katcl(d->d_line);
  }

  return -1;
}

struct katcl_line *line_katcp(struct katcp_dispatch *d)
{
  if(d == NULL){
    return NULL;
  }

  return d->d_line;
}

int flushing_katcp(struct katcp_dispatch *d)
{
  if(d && d->d_line){
    return flushing_katcl(d->d_line);
  }

  return 0;
}

#if 0
int flush_katcp(struct katcp_dispatch *d)
{
  fd_set fsw;
  struct timeval tv;
  int result, fd;

  /* WARNING: not quite sure if unexpected flushes break some assumptions in main loop */

  if((d == NULL) || (d->d_line == NULL)){
#ifdef DEBUG
    fprintf(stderr, "flush: broken state\n");
#endif
    return -1;
  }

  if(!flushing_katcl(d->d_line)){
#ifdef DEBUG
    fprintf(stderr, "flush: nothing to do\n");
#endif
    return 1;
  }

  tv.tv_sec = 0;
  tv.tv_usec = 1;

  fd = fileno_katcl(d->d_line);

  FD_ZERO(&fsw);
  FD_SET(fd, &fsw);

  result = select(fd + 1, NULL, &fsw, NULL, &tv);
  if(result <= 0){
    return result;
  }

  /* returns zero if still more to flush */
  
  return write_katcl(d->d_line);
}
#endif

int write_katcp(struct katcp_dispatch *d)
{
  if(d && d->d_line){
    return write_katcl(d->d_line);
  }
  
  return -1;
}

int read_katcp(struct katcp_dispatch *d)
{
  if(d && d->d_line){
    return read_katcl(d->d_line);
  }

  return -1;
}

int error_katcp(struct katcp_dispatch *d)
{
  if(d && d->d_line){
    return error_katcl(d->d_line);
  }
  
  return EINVAL;
}

void resume_katcp(struct katcp_dispatch *d)
{
  d->d_pause = 0;
}

int exited_katcp(struct katcp_dispatch *d)
{
  if(d == NULL){
    return KATCP_EXIT_ABORT;
  }

  if(d->d_run > 0){
    return KATCP_EXIT_NOTYET;
  }

  return d->d_exit;
}

int exiting_katcp(struct katcp_dispatch *d)
{
  if(d){
    if(d->d_run == (-1)){
      d->d_run = 0;
      return 1;
    }
  }
  
  return 0;
}

int terminate_katcp(struct katcp_dispatch *d, int code)
{
  if(d == NULL){
    return -1;
  }

  d->d_run = (-1);

  if(code < 0){
#ifdef REPORT
    fprintf(stderr, "terminate: logic failure: values no longer negative\n");
#endif
    d->d_exit = KATCP_EXIT_ABORT;
  } else {
    d->d_exit = code;
  }

  if(d->d_exit == KATCP_EXIT_ABORT){
    return -1;
  }

  return 0;
}

#if 0
int running_katcp(struct katcp_dispatch *d)
{
  if(d){
    return d->d_run;
  }
  
  return 0;
}
#endif

void reset_katcp(struct katcp_dispatch *d, int fd)
{
  if(d == NULL){
    return;
  }

  destroy_nonsensors_katcp(d);

  disown_notices_katcp(d);

  if(d->d_end){
    /* TODO: record exit information via a parse */
    wake_notice_katcp(d, d->d_end, NULL);
  }

  d->d_run = 1;
  d->d_exit = KATCP_EXIT_ABORT; /* assume the worst */
  d->d_pause = 0;

  d->d_level = KATCP_LEVEL_INFO; /* back to default */

  if(d->d_line){
    exchange_katcl(d->d_line, fd);
  }
}

/**************************************************************/

int forget_cmd_katcp(struct katcp_dispatch *d, int argc)
{
  char *match;

  if(d->d_shared == NULL){
    return KATCP_RESULT_FAIL;
  }

  if(argc < 2){
    extra_response_katcp(d, KATCP_RESULT_INVALID, "usage");
    return KATCP_RESULT_OWN;
  }

  match = arg_string_katcp(d, 1);
  if(match == NULL){
    log_message_katcp(d, KATCP_LEVEL_ERROR, NULL, "need a command to forget");
    return KATCP_RESULT_FAIL;
  }

  if(deregister_command_katcp(d, match) < 0){
    log_message_katcp(d, KATCP_LEVEL_ERROR, NULL, "unable to deregister command");
    return KATCP_RESULT_FAIL;
  }

  return KATCP_RESULT_OK;
}

int chdir_cmd_katcp(struct katcp_dispatch *d, int argc)
{
  char *dir;

  if(d->d_shared == NULL){
    return KATCP_RESULT_FAIL;
  }

  if(argc < 2){
    extra_response_katcp(d, KATCP_RESULT_INVALID, "usage");
    return KATCP_RESULT_OWN;
  }

  dir = arg_string_katcp(d, 1);
  if(dir == NULL){
    log_message_katcp(d, KATCP_LEVEL_ERROR, NULL, "unable to retrive directory argument");
    return KATCP_RESULT_FAIL;
  }

  if(chdir(dir) < 0){
    log_message_katcp(d, KATCP_LEVEL_ERROR, NULL, "unable to change to %s %s", dir, strerror(errno));
    return KATCP_RESULT_FAIL;
  }

  return KATCP_RESULT_OK;
}

int setenv_cmd_katcp(struct katcp_dispatch *d, int argc)
{
  char *label, *value;

  if(d->d_shared == NULL){
    return KATCP_RESULT_FAIL;
  }

  if(argc < 2){
    extra_response_katcp(d, KATCP_RESULT_INVALID, "usage");
    return KATCP_RESULT_OWN;
  }

  label = arg_string_katcp(d, 1);
  if(argc == 2){
    if(unsetenv(label) < 0){
      log_message_katcp(d, KATCP_LEVEL_ERROR, NULL, "unable to clear variable %s : %s", label, strerror(errno));
      return KATCP_RESULT_FAIL;
    }
    return KATCP_RESULT_OK;
  }

  value = arg_string_katcp(d, 2);
  if(value == NULL){
    log_message_katcp(d, KATCP_LEVEL_ERROR, NULL, "unable to acquire value for variable %s", label);
    return KATCP_RESULT_FAIL;
  }

  if(setenv(label, value, 1) < 0){
    log_message_katcp(d, KATCP_LEVEL_ERROR, NULL, "unable to set variable %s : %s", label, strerror(errno));
    return KATCP_RESULT_FAIL;
  }

  return KATCP_RESULT_OK;
}

int help_cmd_katcp(struct katcp_dispatch *d, int argc)
{
  struct katcp_cmd *c;
  unsigned long count;
  char *match;

  if(d->d_shared == NULL){
    return KATCP_RESULT_FAIL;
  }

  count = 0;
  match = (argc <= 1) ? NULL : arg_string_katcp(d, 1);

#ifdef DEBUG
  fprintf(stderr, "printing help\n");
#endif

  log_message_katcp(d, KATCP_LEVEL_TRACE, NULL, "printing available commands for mode %d", d->d_shared->s_mode);

  for(c = d->d_shared->s_commands; c; c = c->c_next){
    if((c->c_mode == 0) || (d->d_shared->s_mode == c->c_mode)){
      if((c->c_name) && (
          ((match != NULL) && (!strcmp(c->c_name + 1, match))) || /* print matching command for this mode, no matter if hidden or not */
          ((match == NULL) && (((c->c_flags & KATCP_CMD_HIDDEN) == 0))) /* print all not hidden commands in this mode */
      )){

        prepend_inform_katcp(d);
        append_string_katcp(d, KATCP_FLAG_STRING, c->c_name + 1);
        append_string_katcp(d, KATCP_FLAG_STRING | KATCP_FLAG_LAST, c->c_help);

#if 0
        send_katcp(d, KATCP_FLAG_FIRST | KATCP_FLAG_STRING, "#help", KATCP_FLAG_STRING, c->c_name + 1, KATCP_FLAG_LAST | KATCP_FLAG_STRING, c->c_help);
#endif

        count++;
      }
    }
  }

  if(count > 0){
    prepend_reply_katcp(d);
    append_string_katcp(d, KATCP_FLAG_STRING, KATCP_OK);
    append_unsigned_long_katcp(d, KATCP_FLAG_ULONG | KATCP_FLAG_LAST, count);

#if 0
    send_katcp(d, 
      KATCP_FLAG_FIRST | KATCP_FLAG_STRING, "!help", 
                         KATCP_FLAG_STRING, KATCP_OK,
      KATCP_FLAG_LAST  | KATCP_FLAG_ULONG, (unsigned long) count);
#endif

  } else {
    extra_response_katcp(d, KATCP_RESULT_INVALID, "request");
#if 0
    send_katcp(d, 

      KATCP_FLAG_FIRST | KATCP_FLAG_STRING, "!help", 
                         KATCP_FLAG_STRING, KATCP_INVALID,
      KATCP_FLAG_LAST  | KATCP_FLAG_STRING, "unknown request");
#endif
  }
  
  return KATCP_RESULT_OWN;
}

int halt_cmd_katcp(struct katcp_dispatch *d, int argc)
{
  terminate_katcp(d, KATCP_EXIT_HALT);

  return KATCP_RESULT_OK;
}

int restart_cmd_katcp(struct katcp_dispatch *d, int argc)
{
  terminate_katcp(d, KATCP_EXIT_RESTART);

  return KATCP_RESULT_OK;
}

int watchdog_cmd_katcp(struct katcp_dispatch *d, int argc)
{
  return KATCP_RESULT_OK;
}

int name_log_level_katcp(struct katcp_dispatch *d, char *name)
{
  int level;

  if(name == NULL){
    return d->d_level;
  }

  level = log_to_code_katcl(name);
  if(level < 0){
    return -1;
  }

  d->d_level = level;

#ifdef DEBUG
  fprintf(stderr, "log: set log level to %s (%d)\n", name, level);
#endif

  return d->d_level;
}

int log_level_katcp(struct katcp_dispatch *d, unsigned int level)
{
  if(level >= KATCP_MAX_LEVELS){
    return -1;
  }

  d->d_level = level;

  return d->d_level;
}

int log_level_cmd_katcp(struct katcp_dispatch *d, int argc)
{
  int ok, code;
  char *requested, *got;

  if(argc > 1){
    ok = 0;
    requested = arg_string_katcp(d, 1);

    if(requested){
      if(!strcmp(requested, "all")){
        ok = 1;
        d->d_level = KATCP_LEVEL_TRACE;
      }
      code = log_to_code_katcl(requested);
      if(code >= 0){
        ok = 1;
        d->d_level = code;
      }
    }
  } else {
    ok = 1;
    requested = NULL;
  }

  got = log_to_string_katcl(d->d_level);
  if(got == NULL){
    ok = 0;
  }

  if(ok){

    prepend_reply_katcp(d);
    append_string_katcp(d, KATCP_FLAG_STRING, KATCP_OK);
    append_string_katcp(d, KATCP_FLAG_STRING | KATCP_FLAG_LAST, got);

#if 0
    send_katcp(d, 
      KATCP_FLAG_FIRST | KATCP_FLAG_STRING, "!log-level", 
                         KATCP_FLAG_STRING, KATCP_OK,
      KATCP_FLAG_LAST  | KATCP_FLAG_STRING, got);
#endif

  } else {
    if(requested){
      log_message_katcp(d, KATCP_LEVEL_WARN, NULL, "unknown log level %s", requested);
    }
    extra_response_katcp(d, KATCP_RESULT_INVALID, "level");
  }

  return KATCP_RESULT_OWN;
}

int log_record_cmd_katcp(struct katcp_dispatch *d, int argc)
{
  int code;
  char *message, *level;

  code = KATCP_LEVEL_INFO;

  if(argc > 2){
    message = arg_string_katcp(d, 2);
    level   = arg_string_katcp(d, 1);
    if(level){
      code = log_to_code_katcl(level);
      if(code < 0){
        code = KATCP_LEVEL_INFO;
      }
    }
  } else if(argc > 1){
    message = arg_string_katcp(d, 1);
  } else {
    log_message_katcp(d, KATCP_LEVEL_ERROR, NULL, "require a message to log");
    return KATCP_RESULT_FAIL;
  }

  if(message == NULL){
    log_message_katcp(d, KATCP_LEVEL_FATAL, NULL, "unable to acquire message");
    return KATCP_RESULT_FAIL;
  }

  log_message_katcp(d, code, NULL, "%s", message);

  return KATCP_RESULT_OK;
}

/**************************************************************/

int broadcast_inform_katcp(struct katcp_dispatch *d, char *name, char *arg)
{
  struct katcp_shared *s;
  int result, sum, i;

  sane_katcp(d);

  s = d->d_shared;
  if(s == NULL){
    return -1;
  }
  
  sum = 0;
  for(i = 0; i < s->s_used; i++){
    d = s->s_clients[i];
    if(d->d_line){
      result = basic_inform_katcp(d, name, arg);
      if(result < 0){
        return -1;
      }
      sum += result;
    }
  }
 
  return sum;
}

int log_relay_katcp(struct katcp_dispatch *d, struct katcl_parse *p)
{
  struct katcp_shared *s;
  char *priority;
  unsigned int i;
  int code, sum, result;

  sane_katcp(d);

  code = (-1);
  priority = get_string_parse_katcl(p, 1);

  if(priority){
    code = log_to_code_katcl(priority);
  }

  if(code < 0){
    /* TODO: malformed log message, maybe resport this ? */
    return -1;
  }

  s = d->d_shared;
  if(s == NULL){
#ifdef REPORT
    fprintf(stderr, "log: no shared state available\n");
#endif
    return -1;
  }

  if(s->s_count > 0){ /* do we have clones ? Then send it to them */
    if(s->s_template != d){ /* only honour local messages if we are not the template (which doesn't do IO) */
      if(code & KATCP_LEVEL_LOCAL){
        s = NULL;
      }
    }
  } else { /* running with a single dispatch, no clones */
    s = NULL;
  }

#if DEBUG > 2
  fprintf(stderr, "log: sending log message to %s\n", s ? "all" : "single");
#endif

  result = 0;

  if(s){
    for(i = 0; i < s->s_used; i++){
      d = s->s_clients[i];
      if(code >= d->d_level){
        sum = append_parse_katcl(d->d_line, p);
        if(result < 0){
          sum = result;
        } else {
          if(sum >= 0){
            sum += result;
          }
        }
      }
    }
  } else {
    if(code >= d->d_level){
      result = append_parse_katcl(d->d_line, p);
    }
  }

  return result;
}

int log_message_katcp(struct katcp_dispatch *d, unsigned int priority, char *name, char *fmt, ...)
{
  va_list args;
  int result, sum;
  unsigned int level, i;
  struct katcp_shared *s;
  struct katcp_entry *e;
  char *fixed_name;

  level = priority & KATCP_MASK_LEVELS;
  sum = 0;

  sane_katcp(d);

  s = d->d_shared;
  if(s == NULL){
#ifdef REPORT
    fprintf(stderr, "log: no shared state available\n");
#endif
    return -1;
  }

  if(name){
    fixed_name = name;
  } else {
    e = &(s->s_vector[s->s_mode]);
    if(e->e_name){
      fixed_name = e->e_name;
    } else {
      /* WARNING: not the most elegant option ... */
#if 0
      fixed_name = "tcpborphserver";
#endif
      fixed_name = KATCP_CODEBASE_NAME;
    }
  }

  if(s->s_count > 0){ /* do we have clones ? Then send it to them */
    if(s->s_template != d){ /* only honour local messages if we are not the template (which doesn't do IO) */
      if(priority & KATCP_LEVEL_LOCAL){
        s = NULL;
      }
    }
  } else { /* running with a single dispatch, no clones */
    s = NULL;
  }

  if(s){
    for(i = 0; i < s->s_used; i++){
      d = s->s_clients[i];
      if(level >= d->d_level){
        va_start(args, fmt);
        result = vlog_message_katcl(d->d_line, level, fixed_name, fmt, args);
        va_end(args);
        if(result < 0){
          sum = result;
        } else {
          if(sum >= 0){
            sum += result;
          }
        }
      }
    }
  } else {
    if(level >= d->d_level){
      va_start(args, fmt);
      sum = vlog_message_katcl(d->d_line, level, fixed_name, fmt, args);
      va_end(args);
    }
  }
 
  return sum;
}

int extra_response_katcp(struct katcp_dispatch *d, int code, char *fmt, ...)
{
  va_list args;
#if 0
  int result;
#endif
  char *result;

  if(code > KATCP_RESULT_OK){
    return 0;
  }

#if 0
  va_start(args, fmt);
  result = vextra_response_katcl(d->d_line, code, fmt, args);
  va_end(args);
#endif

  result = code_to_name_katcm(code);
  if(result == NULL){
    result = KATCP_INVALID;
  }

  prepend_reply_katcp(d);

  if(fmt != NULL){
    append_string_katcp(d, KATCP_FLAG_STRING, result);
    va_start(args, fmt);
    append_vargs_katcp(d, KATCP_FLAG_LAST, fmt, args);
    va_end(args);
  } else {
    append_string_katcp(d, KATCP_FLAG_STRING | KATCP_FLAG_LAST, result);
  }

  return 0;
}

int basic_inform_katcp(struct katcp_dispatch *d, char *name, char *arg)
{
  if(arg){
    append_string_katcp(d, KATCP_FLAG_STRING | KATCP_FLAG_FIRST, name);
    append_string_katcp(d, KATCP_FLAG_STRING | KATCP_FLAG_LAST, arg);
  } else {
    append_string_katcp(d, KATCP_FLAG_STRING | KATCP_FLAG_FIRST | KATCP_FLAG_LAST, name);
  }

  return 0;
}

int arg_request_katcp(struct katcp_dispatch *d)
{
  sane_katcp(d);

  return arg_request_katcl(d->d_line);
}

int arg_reply_katcp(struct katcp_dispatch *d)
{
  sane_katcp(d);

  return arg_reply_katcl(d->d_line);
}

int arg_inform_katcp(struct katcp_dispatch *d)
{
  sane_katcp(d);

  return arg_inform_katcl(d->d_line);
}

int arg_null_katcp(struct katcp_dispatch *d, unsigned int index)
{
  sane_katcp(d);

  return arg_null_katcl(d->d_line, index);
}

unsigned int arg_count_katcp(struct katcp_dispatch *d)
{
  sane_katcp(d);

  return arg_count_katcl(d->d_line);
}

char *arg_string_katcp(struct katcp_dispatch *d, unsigned int index)
{
  sane_katcp(d);

  return arg_string_katcl(d->d_line, index);
}

char *arg_copy_string_katcp(struct katcp_dispatch *d, unsigned int index)
{
  sane_katcp(d);

  return arg_copy_string_katcl(d->d_line, index);
}

unsigned long arg_unsigned_long_katcp(struct katcp_dispatch *d, unsigned int index)
{
  sane_katcp(d);

  return arg_unsigned_long_katcl(d->d_line, index);
}

#ifdef KATCP_USE_FLOATS
double arg_double_katcp(struct katcp_dispatch *d, unsigned int index)
{
  sane_katcp(d);

  return arg_double_katcl(d->d_line, index);
}
#endif

unsigned int arg_buffer_katcp(struct katcp_dispatch *d, unsigned int index, void *buffer, unsigned int size)
{
  sane_katcp(d);

  /* returns the number of bytes it wanted to copy, more than size indicates a failure */
  return arg_buffer_katcl(d->d_line, index, buffer, size);
}

/**************************************************************/

static int prepend_generic_katcp(struct katcp_dispatch *d, int reply)
{
  char *message, *string;
  int result;

  message = arg_string_katcl(d->d_line, 0);
  if((message == NULL) || (message[0] != KATCP_REQUEST)){
#ifdef REPORT
    fprintf(stderr, "prepend: arg0 is unavailable (%p)\n", message);
#endif
    return -1;
  }

  string = strdup(message);
  if(string == NULL){
    return -1;
  }
  string[0] = reply ? KATCP_REPLY : KATCP_INFORM;

  result = append_string_katcp(d, KATCP_FLAG_FIRST | KATCP_FLAG_STRING, string);
  free(string);

  return result;
}

int prepend_reply_katcp(struct katcp_dispatch *d)
{
  return prepend_generic_katcp(d, 1);
}

int prepend_inform_katcp(struct katcp_dispatch *d)
{
  return prepend_generic_katcp(d, 0);
}

int append_string_katcp(struct katcp_dispatch *d, int flags, char *buffer)
{
  sane_katcp(d);

  return append_string_katcl(d->d_line, flags, buffer);
}

int append_unsigned_long_katcp(struct katcp_dispatch *d, int flags, unsigned long v)
{
  sane_katcp(d);

  return append_unsigned_long_katcl(d->d_line, flags, v);
}

int append_signed_long_katcp(struct katcp_dispatch *d, int flags, unsigned long v)
{
  sane_katcp(d);

  return append_signed_long_katcl(d->d_line, flags, v);
}

int append_hex_long_katcp(struct katcp_dispatch *d, int flags, unsigned long v)
{
  sane_katcp(d);

  return append_hex_long_katcl(d->d_line, flags, v);
}

#ifdef KATCP_USE_FLOATS
int append_double_katcp(struct katcp_dispatch *d, int flags, double v)
{
  sane_katcp(d);

  return append_double_katcl(d->d_line, flags, v);
}
#endif

int append_buffer_katcp(struct katcp_dispatch *d, int flags, void *buffer, int len)
{
  sane_katcp(d);

  return append_buffer_katcl(d->d_line, flags, buffer, len);
}

int append_parameter_katcp(struct katcp_dispatch *d, int flags, struct katcl_parse *p, unsigned int index)
{
  sane_katcp(d);

  return append_parameter_katcl(d->d_line, flags, p, index);
}

int append_parse_katcp(struct katcp_dispatch *d, struct katcl_parse *p)
{
  sane_katcp(d);

  return append_parse_katcl(d->d_line, p);
}

int append_vargs_katcp(struct katcp_dispatch *d, int flags, char *fmt, va_list args)
{
  int result;
  va_list copy;

  sane_katcp(d);

  /* NOTE: not sure if this level of paranoia is warranted */
  /* not copying vargs at works on my platform, but not on */
  /* all of them, so not sure how much copying is really needed */

  va_copy(copy, args);
  result = append_vargs_katcl(d->d_line, flags, fmt, copy);
  va_end(copy);

  return result;
}

int append_args_katcp(struct katcp_dispatch *d, int flags, char *fmt, ...)
{
  int result;
  va_list args;

  sane_katcp(d);

  va_start(args, fmt);
  result = append_vargs_katcl(d->d_line, flags, fmt, args);
  va_end(args);

  return result;
}

/**************************************************************/

/* This function is *NOT* a printf style function, instead of
 * a format string, each value to be output is preceeded by
 * a flag field describing its type and if it is the last/first field.
 * The first flag field needs to or in  KATCP_FLAG_FIRST,
 * the last one has to contain KATCP_FLAG_LAST. In the case
 * of binary output, it is necessary to include a further parameter
 * containing the length of the value. Consider this function to 
 * be a convenience which allows one perform multiple append_*_katcp 
 * calls in one go
 */

int vsend_katcp(struct katcp_dispatch *d, va_list args)
{
  int result;
  va_list copy;

  sane_katcp(d);

  /* NOTE: not sure if this level of paranoia is warranted */
  /* not copying vargs at works on my platform, but not on */
  /* all of them, so not sure how much copying is really needed */

  va_copy(copy, args);
  result = vsend_katcl(d->d_line, copy);
  va_end(copy);

  return result;
}

int send_katcp(struct katcp_dispatch *d, ...)
{
  int result;
  va_list args;

  sane_katcp(d);

  va_start(args, d);
  result = vsend_katcl(d->d_line, args);
  va_end(args);

  return result;
}

/**************************************************************/

int dispatch_cmd_katcp(struct katcp_dispatch *d, int argc)
{
  struct katcp_shared *s;
  struct katcp_dispatch *dx;
  unsigned int i;
  char *name, *level;

  s = d->d_shared;

  if(s == NULL){
    return KATCP_RESULT_FAIL;
  }

  name = arg_string_katcp(d, 1);
  if(name == NULL){
    return KATCP_RESULT_FAIL;
  } 

  if(!strcmp(name, "list")){
    for(i = 0; i < s->s_used; i++){
      dx = s->s_clients[i];
      level = log_to_string_katcl(dx->d_level);

      log_message_katcp(d, KATCP_LEVEL_INFO, NULL, "dispatch %s (%s) logging %s with %d notices and %d sensors", dx->d_name, (dx == d) ? "this" : "other", level ? level : "unknown", dx->d_count, dx->d_size);
    }
    log_message_katcp(d, KATCP_LEVEL_INFO, NULL, "%d clients", s->s_number);
    return KATCP_RESULT_OK;
  } else {
    return KATCP_RESULT_FAIL;
  }
}
