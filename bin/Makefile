# This make file builds list_trace: an application to read the content of the
# Slurm simulator traces. To work it requires the presence of list_trace.c,
# sim_trace.c and sim_trace.h in their version present the 0.1 release of the
# ScSF Slurm simulator. The file was not included in this release to avoid
# license problems.

CC=gcc
CFLAGS=-DSLURM_SIMULATOR=1 -I.
DEPS = sim_trace.h


%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

list_trace: sim_trace.o list_trace.o
	gcc -o list_trace sim_trace.o list_trace.o $(CFLAGS)