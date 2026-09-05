#include <arm_acle.h>
#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define READ_SIZE	(1 << 23)
#ifndef MAX_THREAD
#define MAX_THREAD	1
#endif /* MAX_THREAD */
#define MAX_LINE_LEN	107
#define MAX_CAPACITY	(1 << 15)

#define SEMICOLON_BROADCAST	UINT64_C(0x3B3B3B3B3B3B3B3B)
#define LO_MAGIC		UINT64_C(0x0101010101010101)
#define HI_MAGIC		UINT64_C(0x8080808080808080)

struct station
{
	int64_t sum;
	uint32_t hash;
	int16_t max;
	int16_t min;
	int32_t cnt;
	uint8_t nname;
	uint8_t name[100];
};
_Static_assert(sizeof(struct station) == 128, "station must be 128 bytes");

struct data
{
	char *file;
	struct station *stn;
	uint8_t *buf;
	ptrdiff_t cap;
	ptrdiff_t len;
	ptrdiff_t off;
};

static struct station g_stations[MAX_THREAD][MAX_CAPACITY];
static uint8_t g_readbuffers[MAX_THREAD][READ_SIZE];
static pthread_t g_threads[MAX_THREAD];
static struct data g_data[MAX_THREAD];

static struct station *
find(uint8_t *name, uint8_t nname, uint32_t hash, struct station *stn)
{
	uint32_t i = hash & (MAX_CAPACITY - 1);
	for (;;) {
		if (!stn[i].cnt) {
			stn[i].max = INT16_MIN;
			stn[i].min = INT16_MAX;
			stn[i].sum = 0;
			memcpy(stn[i].name, name, nname);
			stn[i].nname = nname;
			stn[i].hash = hash;
			break;
		} else if (stn[i].nname == nname && stn[i].hash == hash && memcmp(stn[i].name, name, nname) == 0) {
			break;
		} else {
			i = (i + 1) & (MAX_CAPACITY - 1);
		}
	}
	return &stn[i];
}

static void
processlines(uint8_t *beg, uint8_t *end, struct station *stations)
{
	uint8_t *cur = beg;
	while (cur < end) {
		uint8_t *name = cur;
		uint32_t hash = 0;
		for (;;) {
			uint64_t word;
			memcpy(&word, cur, 8);
			uint64_t xor = word ^ SEMICOLON_BROADCAST;
			uint64_t match = (xor - LO_MAGIC) & ~xor & HI_MAGIC;
			if (match) {
				uint32_t pos = (uint32_t)(__builtin_ctzll(match) >> 3);
				for (uint32_t i = 0; i < pos; i++)
					hash = __crc32cb(hash, cur[i]);
				cur += pos;
				break;
			}
			hash = __crc32cd(hash, word);
			cur += 8;
		}
		uint8_t nname = (uint8_t)(cur - name);
		++cur;
		/* branchless parse of [-]D[D].D into fixed-point int (e.g. "12.3" -> 123) */
		int32_t neg = *cur == '-';
		cur += neg;
		int32_t num = (*cur++ - '0') * 10;
		int32_t deca = *cur != '.';
		num *= 1 + 9 * deca;
		num += (*cur - '0') * 10 * deca;
		cur += deca + 1;
		num += *cur++ - '0';
		num *= 1 - (2 * neg);
		++cur;
		struct station *stn = find(name, nname, hash, stations);
		++stn->cnt;
		stn->max = (int16_t)(stn->max > num ? stn->max : num);
		stn->min = (int16_t)(stn->min < num ? stn->min : num);
		stn->sum += num;
	}
}

static ptrdiff_t
processbuffer(uint8_t *beg, uint8_t *end, bool lookback, struct station *stations)
{
	/* shift the beginning and end of the buffer to only read whole lines */
	if (lookback) {
		beg += MAX_LINE_LEN;
		uint8_t *limit = beg - MAX_LINE_LEN;
		while (beg > limit && *--beg != '\n');
		++beg;
	}
	uint8_t *oldend = end;
	while (end > beg && *--end != '\n');
	++end;
	processlines(beg, end, stations);
	return oldend - end;
}

static bool
processfile(char *file, uint8_t *buf, ptrdiff_t cap, ptrdiff_t len, ptrdiff_t offset, struct station *stations)
{
	FILE *fp = fopen(file, "rb");
	if (!fp) {
		perror(file);
		return false;
	}
	/* Each batch after first needs to contain the characters from the
	 * previous batch to handle a line being split across batches. The
	 * current batch will only read up to the last \n character, which
	 * means some characters in the current batch will be ignored. The next
	 * batch will read characters from the end of the previous batch, at
	 * most one extra whole line.
	 */
	bool lookback = offset > 0;
	if (lookback) {
		if (fseek(fp, (long)(offset - MAX_LINE_LEN), SEEK_SET)) {
			fclose(fp);
			return false;
		}
		len += MAX_LINE_LEN;
	}
	ptrdiff_t left = 0;
	do {
		ptrdiff_t avail = cap - left;
		ptrdiff_t amount = avail < len ? avail : len;
		ptrdiff_t nread = (ptrdiff_t)fread(buf + left, 1, (size_t)amount, fp);
		if (!nread)
			break;
		len -= nread;
		uint8_t *end = buf + nread + left;
		left = processbuffer(buf, end, lookback, stations);
		if (left)
			memmove(buf, end - left, left);
		lookback = false;
	} while (len);
	fclose(fp);
	return true;
}

static int
fmtval(char *buf, int32_t val)
{
	char *p = buf;
	if (val < 0) {
		*p++ = '-';
		val = -val;
	}
	int32_t whole = val / 10;
	if (whole >= 10)
		*p++ = (char)('0' + whole / 10);
	*p++ = (char)('0' + whole % 10);
	*p++ = '.';
	*p++ = (char)('0' + val % 10);
	return (int)(p - buf);
}

static int32_t
divround(int64_t sum, int32_t cnt)
{
	/* IEEE 754 roundTowardPositive (ceiling) */
	if (sum >= 0)
		return (int32_t)((sum + cnt - 1) / cnt);
	else
		return (int32_t)(sum / cnt);
}

static int
compare(const void *a, const void *b)
{
	struct station *x = (struct station *)a;
	struct station *y = (struct station *)b;
	int res;
	if (x->cnt == 0 && y->cnt == 0)
		res = 0;
	else if (x->cnt == 0)
		res = 1;
	else if (y->cnt == 0)
		res = -1;
	else {
		uint8_t n1 = x->nname;
		uint8_t n2 = y->nname;
		uint8_t minn = n1 < n2 ? n1 : n2;
		res = memcmp(x->name, y->name, minn);
		if (res == 0)
			res = (int)n1 - (int)n2;
	}
	return res;
}

static ptrdiff_t
getsize(char *file)
{
	ptrdiff_t size = 0;
	FILE *fp = fopen(file, "rb");
	if (fp) {
		fseeko(fp, 0, SEEK_END);
		ptrdiff_t s = (ptrdiff_t)ftello(fp);
		if (s > 0)
			size = s;
		fclose(fp);
	}
	return size;
}

static struct station *
merge(struct station *first, ptrdiff_t size, ptrdiff_t count)
{
	struct station *res = first;
	struct station *src = first + size;
	ptrdiff_t n = (count - 1) * size;
	for (ptrdiff_t i = 0; i < n; i++, src++) {
		if (src->cnt) {
			struct station *dst = find(src->name, src->nname, src->hash, res);
			dst->cnt += src->cnt;
			dst->sum += src->sum;
			dst->max = (int16_t)(dst->max > src->max ? dst->max : src->max);
			dst->min = (int16_t)(dst->min < src->min ? dst->min : src->min);
		}
	}
	return res;
}

static void *
threadstart(void *arg)
{
	struct data *d = arg;
	if (!processfile(d->file, d->buf, d->cap, d->len, d->off, d->stn))
		return NULL;
	return arg;
}

int
main(int argc, char *argv[])
{
	char *file = argc > 1 ? argv[1] : "measurements.txt";
	ptrdiff_t nfile = getsize(file);
	if (!nfile)
		return 1;
	ptrdiff_t nbatch = nfile / MAX_THREAD;
	nbatch = nbatch < MAX_LINE_LEN ? MAX_LINE_LEN : nbatch;
	ptrdiff_t nthread = nfile / nbatch;
	nthread = nthread < 1 ? 1 : nthread;
	for (ptrdiff_t i = 0, offset = 0; i < nthread; i++, offset += nbatch) {
		g_data[i] = (struct data){
			.file = file,
			.buf  = g_readbuffers[i],
			.cap  = sizeof g_readbuffers[i],
			.len  = nbatch,
			.off  = offset,
			.stn  = g_stations[i]
		};
	}
	ptrdiff_t ntail = nfile - nbatch * nthread;
	g_data[nthread-1].len += ntail;
	for (ptrdiff_t i = 0; i < nthread; i++) {
		if (pthread_create(&g_threads[i], NULL, threadstart, &g_data[i])) {
			fprintf(stderr, "pthread_create failed\n");
			for (ptrdiff_t j = 0; j < i; j++)
				pthread_join(g_threads[j], NULL);
			return 1;
		}
	}
	bool ok = true;
	for (ptrdiff_t i = 0; i < nthread; i++) {
		void *ret;
		if (pthread_join(g_threads[i], &ret)) {
			fprintf(stderr, "pthread_join failed\n");
			ok = false;
		} else if (!ret) {
			ok = false;
		}
	}
	if (!ok)
		return 1;
	struct station *result = merge(g_stations[0], MAX_CAPACITY, nthread);
	qsort(result, MAX_CAPACITY, sizeof *result, compare);
	/* worst case per entry: ", " (2) + name (100) + "=" + 3 values (5 each) + 2 slashes = 120 */
	static char out[MAX_CAPACITY * 120 + 3];
	char *p = out;
	*p++ = '{';
	for (ptrdiff_t i = 0; (result[i].cnt > 0) && (i < MAX_CAPACITY); i++) {
		char str[120];
		char *s = str;
		if (i > 0) {
			*s++ = ',';
			*s++ = ' ';
		}
		memcpy(s, result[i].name, result[i].nname);
		s += result[i].nname;
		*s++ = '=';
		s += fmtval(s, result[i].min);
		*s++ = '/';
		s += fmtval(s, divround(result[i].sum, result[i].cnt));
		*s++ = '/';
		s += fmtval(s, result[i].max);
		ptrdiff_t len = s - str;
		memcpy(p, str, (size_t)len);
		p += len;
	}
	*p++ = '}';
	*p++ = '\n';
	write(STDOUT_FILENO, out, (size_t)(p - out));
}
