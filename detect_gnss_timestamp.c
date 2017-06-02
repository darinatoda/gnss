/*------------------------------------------------------------------------------
* rtklib unit test driver : rinex function
*-----------------------------------------------------------------------------*/
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include "rtklib.h"

int process_nav(nav_t *nav, uint64_t* start, uint64_t *end)
{
	if(nav->nt > 0)
	{
		*start = nav->tec[0].time.time;
		*end = nav->tec[nav->nt - 1].time.time;
		return 0;
	}else if(nav->ne > 0)
	{
		*start = nav->peph[0].time.time;
		*end = nav->peph[nav->ne - 1].time.time;
		return 0;
	}else if(nav->ng > 0)
	{
		*start = nav->geph[0].toe.time;
		*end = nav->geph[nav->ng - 1].toe.time;
		return 0;
	}else if(nav->ns > 0)
	{
		*start = nav->seph[0].t0.time;
		*end = nav->seph[nav->ns - 1].t0.time;
		return 0;
	}else if(nav->n > 0)
	{
		*start = nav->eph[0].toe.time;
		*end = nav->eph[nav->n - 1].toe.time;
		return 0;
	}else if(nav->nc > 0)
	{
		*start = nav->pclk[0].time.time;
		*end = nav->pclk[nav->nc - 1].time.time;
		return 0;
	}
/*
   printf("Data not in rinex format nt:%d n:%d ng:%d ns:%d nc:%d\n", nav->nt, nav->n, nav->ng, nav->ns, nav->nc);
 */return 1;
}

int process_obs(obs_t *obs, uint64_t* start, uint64_t *end)
{
	if(obs->n > 0)
	{
		*start = obs->data[0].time.time;
		*end = obs->data[obs->n - 1].time.time;
		return 0;
	};
	return 1;
}

/* readrnx(), sortobs(), uniqnav()  */
int read_rinex(char *file, uint64_t* start, uint64_t *end)
{
	obs_t obs={0};
	nav_t nav={0};
	sta_t sta={""};
	int result = 1;

	readrnx(file,1,"",&obs,&nav,&sta);
	result = process_nav(&nav, start, end);
	if(!result)
		printf("rinex %lu %lu\n", *start, *end);
	if(result)
	{
		result = process_obs(&obs, start, end);
		if(!result)
			printf("obs %lu %lu\n", *start, *end);
	}

	free(obs.data);
	free(nav.eph);
	free(nav.geph);
	free(nav.seph);
	return result;
}

int read_clk(char *file, uint64_t* start, uint64_t *end)
{
	nav_t nav={0};
	int result;
	readrnxc(file,&nav);
	result = process_nav(&nav, start, end);
	free(nav.pclk);
	if(!result)
		printf("clk %lu %lu\n", *start, *end);
	return result;
}


int read_ionex(char *file, uint64_t* start, uint64_t *end)
{
	nav_t nav={0};
	int result;
	readtec(file,&nav,0);
	result = process_nav(&nav, start, end);
	if(!result)
		printf("ionex %lu %lu\n", *start, *end);
	return result;
}


int read_sp3(char *file, uint64_t* start, uint64_t *end)
{
	nav_t nav={0};
	int result;
	readsp3(file,&nav,0);

	result = process_nav(&nav, start, end);
	if(!result)
		printf("sp3 %lu %lu\n", *start, *end);
	return result;
}

int read_time(char *file, uint64_t* start, uint64_t *end)
{
	if(strstr(file, ".sp3") || strstr(file, ".SP3"))
		return read_sp3(file, start, end);
	if(strstr(file, ".clk") || strstr(file, ".CLK"))
		return read_clk(file, start, end);
	if(read_rinex(file, start, end) && read_ionex(file, start, end) && read_clk(file, start, end) && read_sp3(file, start, end)) ;
	return 0;
}

int main(int argc, char **argv)
{
	uint64_t st = 0, ed = 0;
	if(argc != 2)
	{
		printf("\nUsage: %s GNSS_FILE\n\n", argv[0]);
		return 1;
	}
	read_time(argv[1], &st, &ed);
	return 0;
}
