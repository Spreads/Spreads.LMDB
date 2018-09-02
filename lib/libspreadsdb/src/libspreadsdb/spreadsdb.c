#include <spreadsdb.h>
#include <lmdb.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <stdint.h>

int sdb_cursor_get_lt(MDB_cursor *mc, MDB_val *key, MDB_val *data)
{
	int rc;
	rc = mdb_cursor_get(mc, key, data, MDB_SET_RANGE);
	if (rc == MDB_SUCCESS)
	{
		return mdb_cursor_get(mc, key, data, MDB_PREV);
	}
	else
	{
		rc = mdb_cursor_get(mc, key, data, MDB_LAST);
	}
	return rc;
}

int sdb_cursor_get_le(MDB_cursor *mc, MDB_val *key, MDB_val *data)
{
	int rc;
	MDB_val searchKey = *key;
	rc = mdb_cursor_get(mc, key, data, MDB_SET_RANGE);
	if (rc == MDB_SUCCESS)
	{
		MDB_txn *txn = mdb_cursor_txn(mc);
		MDB_dbi dbi = mdb_cursor_dbi(mc);
		if (mdb_cmp(txn, dbi, &searchKey, key) < 0)
		{
			return mdb_cursor_get(mc, key, data, MDB_PREV);
		};
	}
	else
	{ // if searchKey > last, MDB_SET_RANGE will fail
		rc = mdb_cursor_get(mc, key, data, MDB_LAST);
		if (rc == MDB_SUCCESS)
		{
			return rc;
		};
	}
	return rc; // equal
}

int sdb_cursor_get_eq(MDB_cursor *mc, MDB_val *key, MDB_val *data)
{
	int rc;
	rc = mdb_cursor_get(mc, key, data, MDB_SET_KEY);
	return rc;
}

int sdb_cursor_get_ge(MDB_cursor *mc, MDB_val *key, MDB_val *data)
{
	return mdb_cursor_get(mc, key, data, MDB_SET_RANGE);
}

int sdb_cursor_get_gt(MDB_cursor *mc, MDB_val *key, MDB_val *data)
{
	// need to store initial value before set_range, which overwrites *key
	MDB_val searchKey = *key;
	MDB_txn *txn = mdb_cursor_txn(mc);
	MDB_dbi dbi = mdb_cursor_dbi(mc);
	int rc;
	rc = mdb_cursor_get(mc, key, data, MDB_SET_RANGE);
	if (rc == MDB_SUCCESS)
	{
		if (mdb_cmp(txn, dbi, &searchKey, key) == 0)
		{
			return mdb_cursor_get(mc, key, data, MDB_NEXT);
		};
		return rc; // equal
	}
	return MDB_NOTFOUND;
}

////// FIND DUP /////////

int sdb_cursor_get_lt_dup(MDB_cursor *mc, MDB_val *key, MDB_val *data)
{
	MDB_val searchData = *data;
	MDB_txn *txn = mdb_cursor_txn(mc);
	MDB_dbi dbi = mdb_cursor_dbi(mc);

	int rc;

	rc = mdb_cursor_get(mc, key, data, MDB_GET_BOTH_RANGE);
	if (rc == MDB_SUCCESS && mdb_dcmp(txn, dbi, &searchData, data) <= 0)
	{
		return mdb_cursor_get(mc, key, data, MDB_PREV_DUP);
	}
	else
	{
		rc = mdb_cursor_get(mc, key, data, MDB_SET);
		if (rc != MDB_SUCCESS)
		{
			return rc;
		}

		rc = mdb_cursor_get(mc, key, data, MDB_LAST_DUP);
	}
	return rc;
}

int sdb_cursor_get_le_dup(MDB_cursor *mc, MDB_val *key, MDB_val *data)
{
	// need to store initial value before set_range, which overwrites *key
	MDB_val searchData = *data;
	MDB_txn *txn = mdb_cursor_txn(mc);
	MDB_dbi dbi = mdb_cursor_dbi(mc);
	int rc;
	rc = mdb_cursor_get(mc, key, data, MDB_GET_BOTH_RANGE);
	if (rc == MDB_SUCCESS)
	{
		if (mdb_dcmp(txn, dbi, &searchData, data) < 0)
		{
			rc = mdb_cursor_get(mc, key, data, MDB_PREV_DUP);
		};
	}
	else
	{ // if searchKey > last, MDB_GET_BOTH_RANGE will fail
		rc = mdb_cursor_get(mc, key, data, MDB_SET);
		if (rc != MDB_SUCCESS)
		{
			return rc;
		}
		rc = mdb_cursor_get(mc, key, data, MDB_LAST_DUP);
		if (rc == MDB_SUCCESS)
		{
			return rc;
		};
	}
	return rc; // equal
}

int sdb_cursor_get_eq_dup(MDB_cursor *mc, MDB_val *key, MDB_val *data)
{
	int rc;
	rc = mdb_cursor_get(mc, key, data, MDB_GET_BOTH);
	return rc;
}

int sdb_cursor_get_ge_dup(MDB_cursor *mc, MDB_val *key, MDB_val *data)
{
	return mdb_cursor_get(mc, key, data, MDB_GET_BOTH_RANGE);
}

int sdb_cursor_get_gt_dup(MDB_cursor *mc, MDB_val *key, MDB_val *data)
{
	MDB_val searchData = *data;
	MDB_txn *txn = mdb_cursor_txn(mc);
	MDB_dbi dbi = mdb_cursor_dbi(mc);
	int rc;
	rc = mdb_cursor_get(mc, key, data, MDB_GET_BOTH_RANGE);
	if (rc == MDB_SUCCESS && mdb_dcmp(txn, dbi, &searchData, data) >= 0)
	{
		rc = mdb_cursor_get(mc, key, data, MDB_NEXT_DUP);
		return rc;
	}
	return rc;
}

////// CUSTOM (D)CMP FUNCs /////////

static int mdb_cmp_uint128(const MDB_val *a, const MDB_val *b)
{
#if BYTE_ORDER == LITTLE_ENDIAN
	unsigned short *u, *c;
	int x;

	u = (unsigned short *)((char *)a->mv_data + 16);
	c = (unsigned short *)((char *)b->mv_data + 16);
	do
	{
		x = *--u - *--c;
	} while (!x && u > (unsigned short *)a->mv_data);
	return x;
#else
	unsigned short *u, *c, *end;
	int x;

	end = (unsigned short *)((char *)a->mv_data + 16);
	u = (unsigned short *)a->mv_data;
	c = (unsigned short *)b->mv_data;
	do
	{
		x = *u++ - *c++;
	} while (!x && u < end);
	return x;
#endif
}

static int mdb_cmp_uint96(const MDB_val *a, const MDB_val *b)
{
#if BYTE_ORDER == LITTLE_ENDIAN
	unsigned short *u, *c;
	int x;

	u = (unsigned short *)((char *)a->mv_data + 12);
	c = (unsigned short *)((char *)b->mv_data + 12);
	do
	{
		x = *--u - *--c;
	} while (!x && u > (unsigned short *)a->mv_data);
	return x;
#else
	unsigned short *u, *c, *end;
	int x;

	end = (unsigned short *)((char *)a->mv_data + 12);
	u = (unsigned short *)a->mv_data;
	c = (unsigned short *)b->mv_data;
	do
	{
		x = *u++ - *c++;
	} while (!x && u < end);
	return x;
#endif
}

static int mdb_cmp_uint80(const MDB_val *a, const MDB_val *b)
{
#if BYTE_ORDER == LITTLE_ENDIAN
	unsigned short *u, *c;
	int x;

	u = (unsigned short *)((char *)a->mv_data + 10);
	c = (unsigned short *)((char *)b->mv_data + 10);
	do
	{
		x = *--u - *--c;
	} while (!x && u > (unsigned short *)a->mv_data);
	return x;
#else
	unsigned short *u, *c, *end;
	int x;

	end = (unsigned short *)((char *)a->mv_data + 10);
	u = (unsigned short *)a->mv_data;
	c = (unsigned short *)b->mv_data;
	do
	{
		x = *u++ - *c++;
	} while (!x && u < end);
	return x;
#endif
}

static int mdb_cmp_uint64(const MDB_val *a, const MDB_val *b)
{
#if BYTE_ORDER == LITTLE_ENDIAN
	unsigned short *u, *c;
	int x;

	u = (unsigned short *)((char *)a->mv_data + 8);
	c = (unsigned short *)((char *)b->mv_data + 8);
	do
	{
		x = *--u - *--c;
	} while (!x && u > (unsigned short *)a->mv_data);
	return x;
#else
	unsigned short *u, *c, *end;
	int x;

	end = (unsigned short *)((char *)a->mv_data + 8);
	u = (unsigned short *)a->mv_data;
	c = (unsigned short *)b->mv_data;
	do
	{
		x = *u++ - *c++;
	} while (!x && u < end);
	return x;
#endif
}

static int mdb_cmp_uint48(const MDB_val *a, const MDB_val *b)
{
#if BYTE_ORDER == LITTLE_ENDIAN
	unsigned short *u, *c;
	int x;

	u = (unsigned short *)((char *)a->mv_data + 6);
	c = (unsigned short *)((char *)b->mv_data + 6);
	do
	{
		x = *--u - *--c;
	} while (!x && u > (unsigned short *)a->mv_data);
	return x;
#else
	unsigned short *u, *c, *end;
	int x;

	end = (unsigned short *)((char *)a->mv_data + 6);
	u = (unsigned short *)a->mv_data;
	c = (unsigned short *)b->mv_data;
	do
	{
		x = *u++ - *c++;
	} while (!x && u < end);
	return x;
#endif
}

static int mdb_cmp_uint32(const MDB_val *a, const MDB_val *b)
{
#if BYTE_ORDER == LITTLE_ENDIAN
	unsigned short *u, *c;
	int x;

	u = (unsigned short *)((char *)a->mv_data + 4);
	c = (unsigned short *)((char *)b->mv_data + 4);
	do
	{
		x = *--u - *--c;
	} while (!x && u > (unsigned short *)a->mv_data);
	return x;
#else
	unsigned short *u, *c, *end;
	int x;

	end = (unsigned short *)((char *)a->mv_data + 4);
	u = (unsigned short *)a->mv_data;
	c = (unsigned short *)b->mv_data;
	do
	{
		x = *u++ - *c++;
	} while (!x && u < end);
	return x;
#endif
}

static int mdb_cmp_uint16(const MDB_val *a, const MDB_val *b)
{
#if BYTE_ORDER == LITTLE_ENDIAN
	unsigned short *u, *c;
	int x;

	u = (unsigned short *)((char *)a->mv_data + 2);
	c = (unsigned short *)((char *)b->mv_data + 2);
	do
	{
		x = *--u - *--c;
	} while (!x && u > (unsigned short *)a->mv_data);
	return x;
#else
	unsigned short *u, *c, *end;
	int x;

	end = (unsigned short *)((char *)a->mv_data + 2);
	u = (unsigned short *)a->mv_data;
	c = (unsigned short *)b->mv_data;
	do
	{
		x = *u++ - *c++;
	} while (!x && u < end);
	return x;
#endif
}

/* if first 64 bits are not zero then compare by them only, else ignore them and compare and use next 64 bits  */
static int mdb_cmp_uint64x64(const MDB_val *a, const MDB_val *b)
{
	// Mind 2-bytes alignment of LMDB. Zero check does not depends on endianess
	if ((*(unsigned short *)((char *)a->mv_data) != 0 || *(unsigned short *)((char *)(a->mv_data + 2)) != 0 || *(unsigned short *)((char *)(a->mv_data + 4)) != 0 || *(unsigned short *)((char *)(a->mv_data + 6)) != 0) &&
		(*(unsigned short *)((char *)b->mv_data) != 0 || *(unsigned short *)((char *)(b->mv_data + 2)) != 0 || *(unsigned short *)((char *)(b->mv_data + 4)) != 0 || *(unsigned short *)((char *)(b->mv_data + 6)) != 0))
	{
		return mdb_cmp_uint64(a, b);
	}

#if BYTE_ORDER == LITTLE_ENDIAN
	unsigned short *u, *c;
	int x;

	u = (unsigned short *)((char *)(a->mv_data + 16));
	c = (unsigned short *)((char *)(b->mv_data + 16));
	do
	{
		x = *--u - *--c;
	} while (!x && u > (unsigned short *)((char *)(a->mv_data + 8)));
	return x;
#else
	unsigned short *u, *c, *end;
	int x;

	end = (unsigned short *)((char *)a->mv_data + 16);
	u = (unsigned short *)((char *)a->mv_data + 8);
	c = (unsigned short *)((char *)b->mv_data + 8);
	do
	{
		x = *u++ - *c++;
	} while (!x && u < end);
	return x;
#endif
}

int sdb_set_dupsort_as_uint128(MDB_txn *txn, MDB_dbi dbi)
{
	return mdb_set_dupsort(txn, dbi, mdb_cmp_uint128);
}

int sdb_set_dupsort_as_uint96(MDB_txn *txn, MDB_dbi dbi)
{
	return mdb_set_dupsort(txn, dbi, mdb_cmp_uint96);
}

int sdb_set_dupsort_as_uint80(MDB_txn *txn, MDB_dbi dbi)
{
	return mdb_set_dupsort(txn, dbi, mdb_cmp_uint80);
}

int sdb_set_dupsort_as_uint64(MDB_txn *txn, MDB_dbi dbi)
{
	return mdb_set_dupsort(txn, dbi, mdb_cmp_uint64);
}

int sdb_set_dupsort_as_uint48(MDB_txn *txn, MDB_dbi dbi)
{
	return mdb_set_dupsort(txn, dbi, mdb_cmp_uint48);
}

int sdb_set_dupsort_as_uint32(MDB_txn *txn, MDB_dbi dbi)
{
	return mdb_set_dupsort(txn, dbi, mdb_cmp_uint32);
}

int sdb_set_dupsort_as_uint16(MDB_txn *txn, MDB_dbi dbi)
{
	return mdb_set_dupsort(txn, dbi, mdb_cmp_uint16);
}

int sdb_set_dupsort_as_uint64x64(MDB_txn *txn, MDB_dbi dbi)
{
	return mdb_set_dupsort(txn, dbi, mdb_cmp_uint64x64);
}

int sdb_put(MDB_env *env, MDB_dbi dbi, MDB_val *key, MDB_val *data, unsigned int flags)
{
	int rc;
	MDB_txn *txn;
	rc = mdb_txn_begin(env, NULL, 0, &txn);
	if (rc == MDB_SUCCESS)
	{
		rc = mdb_put(txn, dbi, key, data, flags);
		if (rc == MDB_SUCCESS)
		{
			rc = mdb_txn_commit(txn);
		}
		else
		{
			mdb_txn_abort(txn);
		}
	}
	return rc;
}

// TODO renew/create txn/mc logic in a separate method

int sdb_find_lt_dup(MDB_env *env, MDB_dbi dbi, MDB_txn **txn, MDB_cursor **mc, MDB_val *key, MDB_val *data)
{
	int rc;
	if (*txn == NULL)
	{
		rc = mdb_txn_begin(env, NULL, MDB_RDONLY, txn);
	}
	else
	{
		rc = mdb_txn_renew(*txn);
	}
	if (rc != MDB_SUCCESS)
	{
		return rc;
	}

	if (*mc == NULL)
	{
		rc = mdb_cursor_open(*txn, dbi, mc);
	}
	else
	{
		rc = mdb_cursor_renew(*txn, *mc);
	}

	if (rc != MDB_SUCCESS)
	{
		return rc;
	}

	rc = sdb_cursor_get_lt_dup(*mc, key, data);

	mdb_txn_reset(*txn);

	return rc;
}

int sdb_find_le_dup(MDB_env *env, MDB_dbi dbi, MDB_txn **txn, MDB_cursor **mc, MDB_val *key, MDB_val *data)
{
	int rc;
	if (*txn == NULL)
	{
		rc = mdb_txn_begin(env, NULL, MDB_RDONLY, txn);
	}
	else
	{
		rc = mdb_txn_renew(*txn);
	}
	if (rc != MDB_SUCCESS)
	{
		return rc;
	}

	if (*mc == NULL)
	{
		rc = mdb_cursor_open(*txn, dbi, mc);
	}
	else
	{
		rc = mdb_cursor_renew(*txn, *mc);
	}

	if (rc != MDB_SUCCESS)
	{
		return rc;
	}

	rc = sdb_cursor_get_le_dup(*mc, key, data);

	mdb_txn_reset(*txn);

	return rc;
}

int sdb_find_eq_dup(MDB_env *env, MDB_dbi dbi, MDB_txn **txn, MDB_cursor **mc, MDB_val *key, MDB_val *data)
{
	int rc;
	if (*txn == NULL)
	{
		rc = mdb_txn_begin(env, NULL, MDB_RDONLY, txn);
	}
	else
	{
		rc = mdb_txn_renew(*txn);
	}
	if (rc != MDB_SUCCESS)
	{
		return rc;
	}

	if (*mc == NULL)
	{
		rc = mdb_cursor_open(*txn, dbi, mc);
	}
	else
	{
		rc = mdb_cursor_renew(*txn, *mc);
	}

	if (rc != MDB_SUCCESS)
	{
		return rc;
	}

	rc = sdb_cursor_get_eq_dup(*mc, key, data);

	mdb_txn_reset(*txn);

	return rc;
}

int sdb_find_ge_dup(MDB_env *env, MDB_dbi dbi, MDB_txn **txn, MDB_cursor **mc, MDB_val *key, MDB_val *data)
{
	int rc;
	if (*txn == NULL)
	{
		rc = mdb_txn_begin(env, NULL, MDB_RDONLY, txn);
	}
	else
	{
		rc = mdb_txn_renew(*txn);
	}
	if (rc != MDB_SUCCESS)
	{
		return rc;
	}

	if (*mc == NULL)
	{
		rc = mdb_cursor_open(*txn, dbi, mc);
	}
	else
	{
		rc = mdb_cursor_renew(*txn, *mc);
	}

	if (rc != MDB_SUCCESS)
	{
		return rc;
	}

	rc = sdb_cursor_get_ge_dup(*mc, key, data);

	mdb_txn_reset(*txn);

	return rc;
}

int sdb_find_gt_dup(MDB_env *env, MDB_dbi dbi, MDB_txn **txn, MDB_cursor **mc, MDB_val *key, MDB_val *data)
{
	int rc;
	if (*txn == NULL)
	{
		rc = mdb_txn_begin(env, NULL, MDB_RDONLY, txn);
	}
	else
	{
		rc = mdb_txn_renew(*txn);
	}
	if (rc != MDB_SUCCESS)
	{
		return rc;
	}

	if (*mc == NULL)
	{
		rc = mdb_cursor_open(*txn, dbi, mc);
	}
	else
	{
		rc = mdb_cursor_renew(*txn, *mc);
	}

	if (rc != MDB_SUCCESS)
	{
		return rc;
	}

	rc = sdb_cursor_get_gt_dup(*mc, key, data);

	mdb_txn_reset(*txn);

	return rc;
}