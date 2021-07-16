VERSION=0.3
LIBS=-lzmq -levent -lpthread -lrt  
CXXFLAGS= $(XFLAGS) -g -std=c++0x -D_GNU_SOURCE -O3 $(INCPATHFLAG)
HEADERS= AdaptiveSampler.h barrier.h cmdline.h Connection.h ConnectionStats.h \
 Generator.h log.h mcperf.h util.h AgentStats.h binary_protocol.h \
 config.h ConnectionOptions.h distributions.h KeyGenerator.h \
 HistogramSampler.h LogHistogramSampler.h Operation.h cpu_stat_thread.h 
CFILES= barrier.cc  cmdline.cc  Connection.cc  distributions.cc  \
 Generator.cc  log.cc  mcperf.cc  TestGenerator.cc  util.cc cpu_stat_thread.cc
SRCS=$(HEADERS) $(CFILES) 
OBJS=mcperf.o cmdline.o log.o distributions.o util.o Connection.o Generator.o cpu_stat_thread.o 
DEPFILES=$(CFILES:.cc=.d)
ifdef GNUPLOT
CXXFLAGS += -DGNUPLOT
OBJS += gnuplot_i.o
HEADERS += gnuplot_i.h
SRCS += gnuplot_i.c gnuplot_i.h
endif
ifdef STATIC
LIBS += -lpgm -luuid -ldl
XFLAGS += -static 
endif

mcperf: Makefile $(OBJS)
	export LD_RUN_PATH=$(LIBPATH) && $(CXX) -o mcperf $(XFLAGS) $(OBJS) $(LIBPATHFLAG) $(LIBS)

.PHONY: clean apt-get zip cmdline

clean:
	rm -f *.o *.d mcperf

apt-get:
	-apt install -y uuid uuid-dev libpgm-dev libevent-dev gengetopt
	-apt install -y libzmq libzmq-dev
	-apt install -y libzmq5 libzmq5-dev

cmdline:
	gengetopt --input=cmdline.ggo --show-required
	gengetopt --input=cmdline.ggo --show-required --show-help > README.help
	sed -e '/Command/,$$d' README.md > README.base
	grep "Command" README.md >> README.base
	echo "==================" >> README.base
	cat README.help >> README.md
	rm README.help README.base

zip: memcache-perf.tgz

release: memcache-perf.tgz

memcache-perf.tgz: $(SRCS)
	tar --transform=s,^,memcache-perf-$(VERSION)/, -cvzf memcache-perf-$(VERSION).tgz $(SRCS) *.h Makefile COPYING README.md 

%.d: %.cc
	@set -e; rm -f $@; \
	$(CC) -MM $(CXXFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

dep: $(DEPFILES)

-include $(DEPFILES)


