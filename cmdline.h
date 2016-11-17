/** @file cmdline.h
 *  @brief The header file for the command line option parser
 *  generated by GNU Gengetopt version 2.22.6
 *  http://www.gnu.org/software/gengetopt.
 *  DO NOT modify this file, since it can be overwritten
 *  @author GNU Gengetopt by Lorenzo Bettini */

#ifndef CMDLINE_H
#define CMDLINE_H

/* If we use autoconf.  */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h> /* for FILE */

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#ifndef CMDLINE_PARSER_PACKAGE
/** @brief the program name (used for printing errors) */
#define CMDLINE_PARSER_PACKAGE "mcperf"
#endif

#ifndef CMDLINE_PARSER_PACKAGE_NAME
/** @brief the complete program name (used for help and version) */
#define CMDLINE_PARSER_PACKAGE_NAME "mcperf"
#endif

#ifndef CMDLINE_PARSER_VERSION
/** @brief the program version */
#define CMDLINE_PARSER_VERSION "0.2"
#endif

/** @brief Where the command line options are stored */
struct gengetopt_args_info
{
  const char *help_help; /**< @brief Print help and exit help description.  */
  const char *version_help; /**< @brief Print version and exit help description.  */
  unsigned int verbose_min; /**< @brief Verbosity. Repeat for more verbose.'s minimum occurreces */
  unsigned int verbose_max; /**< @brief Verbosity. Repeat for more verbose.'s maximum occurreces */
  const char *verbose_help; /**< @brief Verbosity. Repeat for more verbose. help description.  */
  const char *quiet_help; /**< @brief Disable log messages. help description.  */
  char ** server_arg;	/**< @brief Memcached server hostname[:port].  Repeat to specify multiple servers..  */
  char ** server_orig;	/**< @brief Memcached server hostname[:port].  Repeat to specify multiple servers. original value given at command line.  */
  unsigned int server_min; /**< @brief Memcached server hostname[:port].  Repeat to specify multiple servers.'s minimum occurreces */
  unsigned int server_max; /**< @brief Memcached server hostname[:port].  Repeat to specify multiple servers.'s maximum occurreces */
  const char *server_help; /**< @brief Memcached server hostname[:port].  Repeat to specify multiple servers. help description.  */
  const char *binary_help; /**< @brief Use binary memcached protocol instead of ASCII. help description.  */
  int qps_arg;	/**< @brief Target aggregate QPS. 0 = peak QPS. (default='0').  */
  char * qps_orig;	/**< @brief Target aggregate QPS. 0 = peak QPS. original value given at command line.  */
  const char *qps_help; /**< @brief Target aggregate QPS. 0 = peak QPS. help description.  */
  int time_arg;	/**< @brief Maximum time to run (seconds). (default='5').  */
  char * time_orig;	/**< @brief Maximum time to run (seconds). original value given at command line.  */
  const char *time_help; /**< @brief Maximum time to run (seconds). help description.  */
  char * keysize_arg;	/**< @brief Length of memcached keys (distribution). (default='30').  */
  char * keysize_orig;	/**< @brief Length of memcached keys (distribution). original value given at command line.  */
  const char *keysize_help; /**< @brief Length of memcached keys (distribution). help description.  */
  char * valuesize_arg;	/**< @brief Length of memcached values (distribution). (default='200').  */
  char * valuesize_orig;	/**< @brief Length of memcached values (distribution). original value given at command line.  */
  const char *valuesize_help; /**< @brief Length of memcached values (distribution). help description.  */
  int records_arg;	/**< @brief Number of memcached records to use.  If multiple memcached servers are given, this number is divided by the number of servers. (default='10000').  */
  char * records_orig;	/**< @brief Number of memcached records to use.  If multiple memcached servers are given, this number is divided by the number of servers. original value given at command line.  */
  const char *records_help; /**< @brief Number of memcached records to use.  If multiple memcached servers are given, this number is divided by the number of servers. help description.  */
  float update_arg;	/**< @brief Ratio of set:get commands. (default='0.0').  */
  char * update_orig;	/**< @brief Ratio of set:get commands. original value given at command line.  */
  const char *update_help; /**< @brief Ratio of set:get commands. help description.  */
  char * username_arg;	/**< @brief Username to use for SASL authentication..  */
  char * username_orig;	/**< @brief Username to use for SASL authentication. original value given at command line.  */
  const char *username_help; /**< @brief Username to use for SASL authentication. help description.  */
  char * password_arg;	/**< @brief Password to use for SASL authentication..  */
  char * password_orig;	/**< @brief Password to use for SASL authentication. original value given at command line.  */
  const char *password_help; /**< @brief Password to use for SASL authentication. help description.  */
  int threads_arg;	/**< @brief Number of threads to spawn. (default='1').  */
  char * threads_orig;	/**< @brief Number of threads to spawn. original value given at command line.  */
  const char *threads_help; /**< @brief Number of threads to spawn. help description.  */
  const char *affinity_help; /**< @brief Set CPU affinity for threads, round-robin help description.  */
  int connections_arg;	/**< @brief Connections to establish per server. (default='1').  */
  char * connections_orig;	/**< @brief Connections to establish per server. original value given at command line.  */
  const char *connections_help; /**< @brief Connections to establish per server. help description.  */
  int depth_arg;	/**< @brief Maximum depth to pipeline requests. (default='1').  */
  char * depth_orig;	/**< @brief Maximum depth to pipeline requests. original value given at command line.  */
  const char *depth_help; /**< @brief Maximum depth to pipeline requests. help description.  */
  const char *roundrobin_help; /**< @brief Assign threads to servers in round-robin fashion.  By default, each thread connects to every server. help description.  */
  char * iadist_arg;	/**< @brief Inter-arrival distribution (distribution).  Note: The distribution will automatically be adjusted to match the QPS given by --qps. (default='exponential').  */
  char * iadist_orig;	/**< @brief Inter-arrival distribution (distribution).  Note: The distribution will automatically be adjusted to match the QPS given by --qps. original value given at command line.  */
  const char *iadist_help; /**< @brief Inter-arrival distribution (distribution).  Note: The distribution will automatically be adjusted to match the QPS given by --qps. help description.  */
  const char *skip_help; /**< @brief Skip transmissions if previous requests are late.  This harms the long-term QPS average, but reduces spikes in QPS after long latency requests. help description.  */
  const char *moderate_help; /**< @brief Enforce a minimum delay of ~1/lambda between requests. help description.  */
  const char *noload_help; /**< @brief Skip database loading. help description.  */
  const char *loadonly_help; /**< @brief Load database and then exit. help description.  */
  const char *blocking_help; /**< @brief Use blocking epoll().  May increase latency. help description.  */
  const char *no_nodelay_help; /**< @brief Don't use TCP_NODELAY. help description.  */
  int warmup_arg;	/**< @brief Warmup time before starting measurement..  */
  char * warmup_orig;	/**< @brief Warmup time before starting measurement. original value given at command line.  */
  const char *warmup_help; /**< @brief Warmup time before starting measurement. help description.  */
  int wait_arg;	/**< @brief Time to wait after startup to start measurement..  */
  char * wait_orig;	/**< @brief Time to wait after startup to start measurement. original value given at command line.  */
  const char *wait_help; /**< @brief Time to wait after startup to start measurement. help description.  */
  char * save_arg;	/**< @brief Record latency samples to given file..  */
  char * save_orig;	/**< @brief Record latency samples to given file. original value given at command line.  */
  const char *save_help; /**< @brief Record latency samples to given file. help description.  */
  char * search_arg;	/**< @brief Search for the QPS where N-order statistic < Xus.  (i.e. --search 95:1000 means find the QPS where 95% of requests are faster than 1000us)..  */
  char * search_orig;	/**< @brief Search for the QPS where N-order statistic < Xus.  (i.e. --search 95:1000 means find the QPS where 95% of requests are faster than 1000us). original value given at command line.  */
  const char *search_help; /**< @brief Search for the QPS where N-order statistic < Xus.  (i.e. --search 95:1000 means find the QPS where 95% of requests are faster than 1000us). help description.  */
  char * scan_arg;	/**< @brief Scan latency across QPS rates from min to max..  */
  char * scan_orig;	/**< @brief Scan latency across QPS rates from min to max. original value given at command line.  */
  const char *scan_help; /**< @brief Scan latency across QPS rates from min to max. help description.  */
  const char *agentmode_help; /**< @brief Run client in agent mode. help description.  */
  char ** agent_arg;	/**< @brief Enlist remote agent..  */
  char ** agent_orig;	/**< @brief Enlist remote agent. original value given at command line.  */
  unsigned int agent_min; /**< @brief Enlist remote agent.'s minimum occurreces */
  unsigned int agent_max; /**< @brief Enlist remote agent.'s maximum occurreces */
  const char *agent_help; /**< @brief Enlist remote agent. help description.  */
  char * agent_port_arg;	/**< @brief Agent port. (default='5556').  */
  char * agent_port_orig;	/**< @brief Agent port. original value given at command line.  */
  const char *agent_port_help; /**< @brief Agent port. help description.  */
  int lambda_mul_arg;	/**< @brief Lambda multiplier.  Increases share of QPS for this client. (default='1').  */
  char * lambda_mul_orig;	/**< @brief Lambda multiplier.  Increases share of QPS for this client. original value given at command line.  */
  const char *lambda_mul_help; /**< @brief Lambda multiplier.  Increases share of QPS for this client. help description.  */
  int measure_connections_arg;	/**< @brief Master client connections per server, overrides --connections..  */
  char * measure_connections_orig;	/**< @brief Master client connections per server, overrides --connections. original value given at command line.  */
  const char *measure_connections_help; /**< @brief Master client connections per server, overrides --connections. help description.  */
  int measure_qps_arg;	/**< @brief Explicitly set master client QPS, spread across threads and connections..  */
  char * measure_qps_orig;	/**< @brief Explicitly set master client QPS, spread across threads and connections. original value given at command line.  */
  const char *measure_qps_help; /**< @brief Explicitly set master client QPS, spread across threads and connections. help description.  */
  int measure_depth_arg;	/**< @brief Set master client connection depth..  */
  char * measure_depth_orig;	/**< @brief Set master client connection depth. original value given at command line.  */
  const char *measure_depth_help; /**< @brief Set master client connection depth. help description.  */
  int poll_freq_arg;	/**< @brief Set frequency in seconds for agent protocol recv polling. (default='1').  */
  char * poll_freq_orig;	/**< @brief Set frequency in seconds for agent protocol recv polling. original value given at command line.  */
  const char *poll_freq_help; /**< @brief Set frequency in seconds for agent protocol recv polling. help description.  */
  int poll_max_arg;	/**< @brief Set timeout for agent protocol recv polling. An agent not responding within time limit will be dropped. (default='120').  */
  char * poll_max_orig;	/**< @brief Set timeout for agent protocol recv polling. An agent not responding within time limit will be dropped. original value given at command line.  */
  const char *poll_max_help; /**< @brief Set timeout for agent protocol recv polling. An agent not responding within time limit will be dropped. help description.  */
  int getq_size_arg;	/**< @brief Size of queue for multiget requests. (default='100').  */
  char * getq_size_orig;	/**< @brief Size of queue for multiget requests. original value given at command line.  */
  const char *getq_size_help; /**< @brief Size of queue for multiget requests. help description.  */
  float getq_freq_arg;	/**< @brief Frequency of multiget requests, 0 for no multi-get, 100 for only multi-get. (default='0.0').  */
  char * getq_freq_orig;	/**< @brief Frequency of multiget requests, 0 for no multi-get, 100 for only multi-get. original value given at command line.  */
  const char *getq_freq_help; /**< @brief Frequency of multiget requests, 0 for no multi-get, 100 for only multi-get. help description.  */
  int keycache_capacity_arg;	/**< @brief Cached key capacity. (default 10000) (default='10000').  */
  char * keycache_capacity_orig;	/**< @brief Cached key capacity. (default 10000) original value given at command line.  */
  const char *keycache_capacity_help; /**< @brief Cached key capacity. (default 10000) help description.  */
  int keycache_reuse_arg;	/**< @brief Number of times to reuse key cache before generating new req sequence. (Default 100) (default='100').  */
  char * keycache_reuse_orig;	/**< @brief Number of times to reuse key cache before generating new req sequence. (Default 100) original value given at command line.  */
  const char *keycache_reuse_help; /**< @brief Number of times to reuse key cache before generating new req sequence. (Default 100) help description.  */
  int keycache_regen_arg;	/**< @brief When regenerating control number of requests to regenerate. (Default 1%) (default='1').  */
  char * keycache_regen_orig;	/**< @brief When regenerating control number of requests to regenerate. (Default 1%) original value given at command line.  */
  const char *keycache_regen_help; /**< @brief When regenerating control number of requests to regenerate. (Default 1%) help description.  */
  
  unsigned int help_given ;	/**< @brief Whether help was given.  */
  unsigned int version_given ;	/**< @brief Whether version was given.  */
  unsigned int verbose_given ;	/**< @brief Whether verbose was given.  */
  unsigned int quiet_given ;	/**< @brief Whether quiet was given.  */
  unsigned int server_given ;	/**< @brief Whether server was given.  */
  unsigned int binary_given ;	/**< @brief Whether binary was given.  */
  unsigned int qps_given ;	/**< @brief Whether qps was given.  */
  unsigned int time_given ;	/**< @brief Whether time was given.  */
  unsigned int keysize_given ;	/**< @brief Whether keysize was given.  */
  unsigned int valuesize_given ;	/**< @brief Whether valuesize was given.  */
  unsigned int records_given ;	/**< @brief Whether records was given.  */
  unsigned int update_given ;	/**< @brief Whether update was given.  */
  unsigned int username_given ;	/**< @brief Whether username was given.  */
  unsigned int password_given ;	/**< @brief Whether password was given.  */
  unsigned int threads_given ;	/**< @brief Whether threads was given.  */
  unsigned int affinity_given ;	/**< @brief Whether affinity was given.  */
  unsigned int connections_given ;	/**< @brief Whether connections was given.  */
  unsigned int depth_given ;	/**< @brief Whether depth was given.  */
  unsigned int roundrobin_given ;	/**< @brief Whether roundrobin was given.  */
  unsigned int iadist_given ;	/**< @brief Whether iadist was given.  */
  unsigned int skip_given ;	/**< @brief Whether skip was given.  */
  unsigned int moderate_given ;	/**< @brief Whether moderate was given.  */
  unsigned int noload_given ;	/**< @brief Whether noload was given.  */
  unsigned int loadonly_given ;	/**< @brief Whether loadonly was given.  */
  unsigned int blocking_given ;	/**< @brief Whether blocking was given.  */
  unsigned int no_nodelay_given ;	/**< @brief Whether no_nodelay was given.  */
  unsigned int warmup_given ;	/**< @brief Whether warmup was given.  */
  unsigned int wait_given ;	/**< @brief Whether wait was given.  */
  unsigned int save_given ;	/**< @brief Whether save was given.  */
  unsigned int search_given ;	/**< @brief Whether search was given.  */
  unsigned int scan_given ;	/**< @brief Whether scan was given.  */
  unsigned int agentmode_given ;	/**< @brief Whether agentmode was given.  */
  unsigned int agent_given ;	/**< @brief Whether agent was given.  */
  unsigned int agent_port_given ;	/**< @brief Whether agent_port was given.  */
  unsigned int lambda_mul_given ;	/**< @brief Whether lambda_mul was given.  */
  unsigned int measure_connections_given ;	/**< @brief Whether measure_connections was given.  */
  unsigned int measure_qps_given ;	/**< @brief Whether measure_qps was given.  */
  unsigned int measure_depth_given ;	/**< @brief Whether measure_depth was given.  */
  unsigned int poll_freq_given ;	/**< @brief Whether poll_freq was given.  */
  unsigned int poll_max_given ;	/**< @brief Whether poll_max was given.  */
  unsigned int getq_size_given ;	/**< @brief Whether getq_size was given.  */
  unsigned int getq_freq_given ;	/**< @brief Whether getq_freq was given.  */
  unsigned int keycache_capacity_given ;	/**< @brief Whether keycache_capacity was given.  */
  unsigned int keycache_reuse_given ;	/**< @brief Whether keycache_reuse was given.  */
  unsigned int keycache_regen_given ;	/**< @brief Whether keycache_regen was given.  */

} ;

/** @brief The additional parameters to pass to parser functions */
struct cmdline_parser_params
{
  int override; /**< @brief whether to override possibly already present options (default 0) */
  int initialize; /**< @brief whether to initialize the option structure gengetopt_args_info (default 1) */
  int check_required; /**< @brief whether to check that all required options were provided (default 1) */
  int check_ambiguity; /**< @brief whether to check for options already specified in the option structure gengetopt_args_info (default 0) */
  int print_errors; /**< @brief whether getopt_long should print an error message for a bad option (default 1) */
} ;

/** @brief the purpose string of the program */
extern const char *gengetopt_args_info_purpose;
/** @brief the usage string of the program */
extern const char *gengetopt_args_info_usage;
/** @brief the description string of the program */
extern const char *gengetopt_args_info_description;
/** @brief all the lines making the help output */
extern const char *gengetopt_args_info_help[];

/**
 * The command line parser
 * @param argc the number of command line options
 * @param argv the command line options
 * @param args_info the structure where option information will be stored
 * @return 0 if everything went fine, NON 0 if an error took place
 */
int cmdline_parser (int argc, char **argv,
  struct gengetopt_args_info *args_info);

/**
 * The command line parser (version with additional parameters - deprecated)
 * @param argc the number of command line options
 * @param argv the command line options
 * @param args_info the structure where option information will be stored
 * @param override whether to override possibly already present options
 * @param initialize whether to initialize the option structure my_args_info
 * @param check_required whether to check that all required options were provided
 * @return 0 if everything went fine, NON 0 if an error took place
 * @deprecated use cmdline_parser_ext() instead
 */
int cmdline_parser2 (int argc, char **argv,
  struct gengetopt_args_info *args_info,
  int override, int initialize, int check_required);

/**
 * The command line parser (version with additional parameters)
 * @param argc the number of command line options
 * @param argv the command line options
 * @param args_info the structure where option information will be stored
 * @param params additional parameters for the parser
 * @return 0 if everything went fine, NON 0 if an error took place
 */
int cmdline_parser_ext (int argc, char **argv,
  struct gengetopt_args_info *args_info,
  struct cmdline_parser_params *params);

/**
 * Save the contents of the option struct into an already open FILE stream.
 * @param outfile the stream where to dump options
 * @param args_info the option struct to dump
 * @return 0 if everything went fine, NON 0 if an error took place
 */
int cmdline_parser_dump(FILE *outfile,
  struct gengetopt_args_info *args_info);

/**
 * Save the contents of the option struct into a (text) file.
 * This file can be read by the config file parser (if generated by gengetopt)
 * @param filename the file where to save
 * @param args_info the option struct to save
 * @return 0 if everything went fine, NON 0 if an error took place
 */
int cmdline_parser_file_save(const char *filename,
  struct gengetopt_args_info *args_info);

/**
 * Print the help
 */
void cmdline_parser_print_help(void);
/**
 * Print the version
 */
void cmdline_parser_print_version(void);

/**
 * Initializes all the fields a cmdline_parser_params structure 
 * to their default values
 * @param params the structure to initialize
 */
void cmdline_parser_params_init(struct cmdline_parser_params *params);

/**
 * Allocates dynamically a cmdline_parser_params structure and initializes
 * all its fields to their default values
 * @return the created and initialized cmdline_parser_params structure
 */
struct cmdline_parser_params *cmdline_parser_params_create(void);

/**
 * Initializes the passed gengetopt_args_info structure's fields
 * (also set default values for options that have a default)
 * @param args_info the structure to initialize
 */
void cmdline_parser_init (struct gengetopt_args_info *args_info);
/**
 * Deallocates the string fields of the gengetopt_args_info structure
 * (but does not deallocate the structure itself)
 * @param args_info the structure to deallocate
 */
void cmdline_parser_free (struct gengetopt_args_info *args_info);

/**
 * The config file parser (deprecated version)
 * @param filename the name of the config file
 * @param args_info the structure where option information will be stored
 * @param override whether to override possibly already present options
 * @param initialize whether to initialize the option structure my_args_info
 * @param check_required whether to check that all required options were provided
 * @return 0 if everything went fine, NON 0 if an error took place
 * @deprecated use cmdline_parser_config_file() instead
 */
int cmdline_parser_configfile (const char *filename,
  struct gengetopt_args_info *args_info,
  int override, int initialize, int check_required);

/**
 * The config file parser
 * @param filename the name of the config file
 * @param args_info the structure where option information will be stored
 * @param params additional parameters for the parser
 * @return 0 if everything went fine, NON 0 if an error took place
 */
int cmdline_parser_config_file (const char *filename,
  struct gengetopt_args_info *args_info,
  struct cmdline_parser_params *params);

/**
 * Checks that all the required options were specified
 * @param args_info the structure to check
 * @param prog_name the name of the program that will be used to print
 *   possible errors
 * @return
 */
int cmdline_parser_required (struct gengetopt_args_info *args_info,
  const char *prog_name);


#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif /* CMDLINE_H */
