//
#define LUA_LIB
// #define LUA_BUILD_AS_DLL

#include "lua.h"
#include "lauxlib.h"

#include <stdlib.h>
#include <string.h>
#include <libcouchbase/couchbase.h>

typedef struct {
	int		valid;
	lcb_t	inst;
} Instance;

#define ENTRY_ERROR_SZ	24
#define RES_ERROR_SZ 	256

typedef struct {
	char			*key;
	lcb_size_t		nkey;
	char			*data;
	lcb_size_t		ndata;
	lcb_uint32_t	flags;
	lcb_cas_t		cas;
	int				responded;
	char			error[ENTRY_ERROR_SZ];
} GetResEntry;

typedef struct {
	int			nentry;
	int			responded;
	char		error[RES_ERROR_SZ];
	GetResEntry	*entries;
} GetRes;

typedef struct {
	char		*key;
	lcb_size_t	nkey;
	lcb_cas_t	cas;
	int			responded;
	int			op;
	char		error[ENTRY_ERROR_SZ];
} StoreResEntry;

typedef struct {
	int				nentry;
	int				responded;
	char			error[RES_ERROR_SZ];
	StoreResEntry	*entries;
} StoreRes;

typedef struct {
	char			*key;
	lcb_size_t		nkey;
	lcb_uint64_t	value;
	lcb_cas_t		cas;
	int				responded;
	char			error[ENTRY_ERROR_SZ];
} ArithResEntry;

typedef struct {
	int				nentry;
	int				responded;
	char			error[RES_ERROR_SZ];
	ArithResEntry	*entries;
} ArithRes;

typedef struct {
	char			*key;
	lcb_size_t		nkey;
	lcb_cas_t		cas;
	int				responded;
	char			error[ENTRY_ERROR_SZ];
} RemoveResEntry;

typedef struct {
	int				nentry;
	int				responded;
	char			error[RES_ERROR_SZ];
	RemoveResEntry	*entries;
} RemoveRes;

typedef struct {
	char			*key;
	lcb_size_t		nkey;
	lcb_cas_t		cas;
	int				responded;
	char			error[ENTRY_ERROR_SZ];
} FlushResEntry;

typedef struct {
	int				nentry;
	int				responded;
	char			error[RES_ERROR_SZ];
	FlushResEntry	*entries;
} FlushRes;

const char* TYPENAME		= "libcouchconn";
const char* GET_RESTYPE		= "libcouchconn_getres";
const char* STORE_RESTYPE	= "libcouchconn_storeres";
const char* ARITH_RESTYPE	= "libcouchconn_arithres";
const char* REMOVE_RESTYPE	= "libcouchconn_removeres";
const char* FLUSH_RESTYPE	= "libcouchconn_flushres";

#define makeFind( Type, EntryType, FieldCount, FieldEntries, FieldKey ) \
	static EntryType* findEntry_##EntryType( Type* res, const char * key, size_t keylen ) {	\
		int i = 0;													\
		for ( i = 0; i < res->FieldCount; i++) {					\
			if (keylen == res->FieldEntries[i].n##FieldKey) {		\
				if (strncmp(res->FieldEntries[i].FieldKey,key,res->FieldEntries[i].n##FieldKey) == 0 ) {\
					return &res->FieldEntries[i];					\
				}													\
			}														\
		}															\
	}

makeFind( GetRes, GetResEntry, nentry, entries, key )
makeFind( StoreRes, StoreResEntry, nentry, entries, key )
makeFind( ArithRes, ArithResEntry, nentry, entries, key )
makeFind( RemoveRes, RemoveResEntry, nentry, entries, key )

static void setShortErrorStr( lcb_error_t error, size_t max, char* ptr ) {
	#define DO_CASE( NAME ) case NAME: { snprintf(ptr,max,#NAME); } break;

	switch(error) {
		DO_CASE(LCB_SUCCESS)
		DO_CASE(LCB_AUTH_CONTINUE)
		DO_CASE(LCB_AUTH_ERROR)
        DO_CASE(LCB_DELTA_BADVAL)
        DO_CASE(LCB_E2BIG)
        DO_CASE(LCB_EBUSY)
        DO_CASE(LCB_EINTERNAL)
        DO_CASE(LCB_EINVAL)
        DO_CASE(LCB_ENOMEM)
        DO_CASE(LCB_ERANGE)
        DO_CASE(LCB_ERROR)
        DO_CASE(LCB_ETMPFAIL)
        DO_CASE(LCB_KEY_EEXISTS)
        DO_CASE(LCB_KEY_ENOENT)
        DO_CASE(LCB_DLOPEN_FAILED)
        DO_CASE(LCB_DLSYM_FAILED)
        DO_CASE(LCB_NETWORK_ERROR)
        DO_CASE(LCB_NOT_MY_VBUCKET)
        DO_CASE(LCB_NOT_STORED)
        DO_CASE(LCB_NOT_SUPPORTED)
        DO_CASE(LCB_UNKNOWN_COMMAND)
        DO_CASE(LCB_UNKNOWN_HOST)
        DO_CASE(LCB_PROTOCOL_ERROR)
        DO_CASE(LCB_ETIMEDOUT)
        DO_CASE(LCB_CONNECT_ERROR)
        DO_CASE(LCB_BUCKET_ENOENT)
        DO_CASE(LCB_CLIENT_ENOMEM)
        DO_CASE(LCB_CLIENT_ETMPFAIL)
        DO_CASE(LCB_EBADHANDLE)
        DO_CASE(LCB_SERVER_BUG)
        DO_CASE(LCB_PLUGIN_VERSION_MISMATCH)
        DO_CASE(LCB_INVALID_HOST_FORMAT)
        DO_CASE(LCB_INVALID_CHAR)
	default:
		snprintf(ptr,max,"UNKNOWN_ERROR");
	}
}

/**
 * The callback function for a "get-style" request.
 *
 * @param instance the instance performing the operation
 * @param cookie the cookie associated with with the command
 * @param error The status of the operation
 * @param resp More information about the actual item (only key
 *             and nkey is valid if error != LCB_SUCCESS)
 */
static void get_cb(lcb_t instance,
					 const void *cookie,
					 lcb_error_t error,
					 const lcb_get_resp_t *resp)
{
	GetRes* res = (GetRes*)(void*)cookie;
	res->responded++;
	switch (resp->version) {
		case 0: {
			GetResEntry *entry = findEntry_GetResEntry( res, resp->v.v0.key, resp->v.v0.nkey );
			if (entry) {
				if (error == LCB_SUCCESS) { 
					entry->data			= (char*)malloc( resp->v.v0.nbytes );
					entry->ndata		= resp->v.v0.nbytes;
					entry->flags		= resp->v.v0.flags;
					entry->cas			= resp->v.v0.cas;
					entry->responded	= 1;
					memcpy( entry->data, resp->v.v0.bytes, resp->v.v0.nbytes );
				}
				else {
					entry->responded	= 1;
					setShortErrorStr(error, ENTRY_ERROR_SZ, entry->error);
				}
			}
			else {
				// error somehow
				snprintf(res->error,RES_ERROR_SZ,"Missing response entry for key [%.*s]",(int)resp->v.v0.nkey,(const char*)resp->v.v0.key);
			}
		}
		break;
		default:
			// error somehow
			snprintf(res->error,RES_ERROR_SZ,"Un coded version struct %d",resp->version);
		break;
	}
}


/**
 * The callback function for a storage request.
 *
 * @param instance the instance performing the operation
 * @param operation the operation performed
 * @param cookie the cookie associated with with the command
 * @param error The status of the operation
 * @param resp More information about the item related to the store
 *             operation. (only key and nkey is valid if
 *             error != LCB_SUCCESS)
 
    typedef struct {
        int version;
        union {
            struct {
                const void *key;
                lcb_size_t nkey;
                lcb_cas_t cas;
            } v0;
        } v;
    } lcb_store_resp_t;
 
	char		*key;
	lcb_size_t	nkey;
	lcb_cas_t	cas;
	int			responded;
	int			op;
	char		error[ENTRY_ERROR_SZ];
 */
static void store_cb(lcb_t instance,
						const void *cookie,
						lcb_storage_t operation,
						lcb_error_t error,
						const lcb_store_resp_t *resp)
{
	StoreRes* res = (StoreRes*)(void*)cookie;
	res->responded++;
	switch (resp->version) {
		case 0: {
			StoreResEntry *entry = findEntry_StoreResEntry( res, resp->v.v0.key, resp->v.v0.nkey );
			if (entry) {
				if (error == LCB_SUCCESS) { 
					entry->cas			= resp->v.v0.cas;
					entry->responded	= 1;
				}
				else {
					entry->responded	= 1;
					setShortErrorStr(error, ENTRY_ERROR_SZ, entry->error);
				}
			}
			else {
				// error somehow
				snprintf(res->error,RES_ERROR_SZ,"Missing response entry for key [%.*s]",(int)resp->v.v0.nkey,(const char*)resp->v.v0.key);
			}
		}
		break;
		default:
			// error somehow
			snprintf(res->error,RES_ERROR_SZ,"Un coded version struct %d",resp->version);
		break;
	}
}

/**
 * The callback function for a remove request.
 *
 * @param instance the instance performing the operation
 * @param cookie the cookie associated with with the command
 * @param error The status of the operation
 * @param resp More information about the operation
 */
/*    
	typedef struct {
        int version;
        union {
            struct {
                const void *key;
                lcb_size_t nkey;
                lcb_cas_t cas;
            } v0;
        } v;
    } lcb_remove_resp_t;

            struct {
                const void *key;
                lcb_size_t nkey;
                lcb_cas_t cas;
                const void *hashkey;
                lcb_size_t nhashkey;
            } v0;
        } v;

*/

static void remove_cb(lcb_t instance,
						const void *cookie,
						lcb_error_t error,
						const lcb_remove_resp_t *resp)
{
	RemoveRes* res = (RemoveRes*)(void*)cookie;
	res->responded++;
	switch (resp->version) {
		case 0: {
			RemoveResEntry *entry = findEntry_RemoveResEntry( res, resp->v.v0.key, resp->v.v0.nkey );
			if (entry) {
				entry->responded = 1;
				if (error == LCB_SUCCESS) { 
					entry->cas = resp->v.v0.cas;
				}
				else {
					setShortErrorStr(error, ENTRY_ERROR_SZ, entry->error);
				}
			}
			else {
				// error somehow
				snprintf(res->error,RES_ERROR_SZ,"Missing response entry for key [%.*s]",(int)resp->v.v0.nkey,(const char*)resp->v.v0.key);
			}
		}
		break;
		default:
			// error somehow
			snprintf(res->error,RES_ERROR_SZ,"Un coded version struct %d",resp->version);
		break;
	}
}

/**
 * The callback function for a touch request.
 *
 * @param instance the instance performing the operation
 * @param cookie the cookie associated with with the command
 * @param error The status of the operation
 * @param resp More information about the operation
 */
static void touch_cb(lcb_t instance,
					   const void *cookie,
					   lcb_error_t error,
					   const lcb_touch_resp_t *resp)
{
}
    
/**
 * The callback function for an arithmetic request.
 *
 * @param instance the instance performing the operation
 * @param cookie the cookie associated with with the command
 * @param error The status of the operation
 * @param resp More information about the operation (only key
 *             and nkey is valid if error != LCB_SUCCESS)
 */
static void arith_cb(lcb_t instance,
						const void *cookie,
						lcb_error_t error,
						const lcb_arithmetic_resp_t *resp)
{
	ArithRes* res = (ArithRes*)(void*)cookie;
	res->responded++;
	switch (resp->version) {
		case 0: {
			ArithResEntry *entry = findEntry_ArithResEntry( res, resp->v.v0.key, resp->v.v0.nkey );
			if (entry) {
				if (error == LCB_SUCCESS) { 
					entry->value		= resp->v.v0.value;
					entry->cas			= resp->v.v0.cas;
					entry->responded	= 1;
				}
				else {
					entry->responded	= 1;
					setShortErrorStr(error, ENTRY_ERROR_SZ, entry->error);
				}
			}
			else {
				// error somehow
				snprintf(res->error,RES_ERROR_SZ,"Missing response entry for key [%.*s]",(int)resp->v.v0.nkey,(const char*)resp->v.v0.key);
			}
		}
		break;
		default:
			// error somehow
			snprintf(res->error,RES_ERROR_SZ,"Un coded version struct %d",resp->version);
		break;
	}
}
    
/**
 * The callback function for a stat request
 *
 * @param instance the instance performing the operation
 * @param cookie the cookie associated with with the command
 * @param error The status of the operation
 * @param resp response data
 */
static void stat_cb(lcb_t instance,
					  const void *cookie,
					  lcb_error_t error,
					  const lcb_server_stat_resp_t *resp)
{
}
    
/**
 * The error callback called when we don't have a request context.
 * This callback may be called when we encounter memory/network
 * error(s), and we can't map it directly to an operation.
 *
 * @param instance The instance that encountered the problem
 * @param error The error we encountered
 * @param errinfo An optional string with more information about
 *                the error (if available)
 */
static void error_cb(lcb_t instance,
					   lcb_error_t error,
					   const char *errinfo)
{
}
    
/**
 * The callback function for a flush request
 *
 * @param instance the instance performing the operation
 * @param cookie the cookie associated with with the command
 * @param error The status of the operation
 * @param resp Response data
 */
static void flush_cb(lcb_t instance,
					   const void *cookie,
					   lcb_error_t error,
					   const lcb_flush_resp_t *resp)
{
	FlushRes* res = (FlushRes*)(void*)cookie;
	res->responded++;
	if (error != LCB_SUCCESS) { 
		setShortErrorStr(error, RES_ERROR_SZ, res->error);
	}
}

// **************************************************************************
// OP DISPATCH  *************************************************************
typedef int (*init_res)(lua_State*,void*pRes,int count,void** pEles);
typedef int (*process_cmd)(lua_State*,int cmdIdx,void* pCmdD,int arg_start,int arg_count,void* pResEleD);
typedef int (*exec_op)(lua_State*,Instance* pInst,void*pCmdD,void**pCmdList,size_t nCmds,void*pResD);

static int Op(lua_State *L
				, size_t cmdSz, size_t argBatchSz
				, size_t resCSz, size_t resEleSz, const char* lua_restype
				, init_res		init_res_cb
				, process_cmd	process_cmd_cb
				, exec_op		exec_op_cb)
{
	lcb_error_t		err			= 0;

	void			*cmdData 	= NULL;
	void			**cmdList	= NULL;
	void			*resData	= NULL;
	void			*resElePtr	= NULL;

	Instance		*pInst		= NULL;

	int				nTop		= 0;
	int				nArgB		= 0;
	int				i			= 0;

	pInst = (Instance*)luaL_checkudata(L,1,TYPENAME);
	if (!pInst || !pInst->valid) {
		return luaL_error(L,"Invalid instance");
	}

	nTop = lua_gettop(L);

	nArgB = 0;
	if ( argBatchSz > 0 ) {
		nArgB = (nTop-1)/argBatchSz;
	}

	if (nArgB < 1 && argBatchSz > 0) {
		lua_pushboolean( L, 0 );
		lua_pushstring( L, "Arg error, nothing to process");
		return 2;
	}

	// Allocate command structures, use lua to auto free things
	size_t allocSz = 0;
	if (nArgB > 0) {
		allocSz = (cmdSz * nArgB) + ( sizeof(*cmdList) * nArgB );
	}
	else {
		allocSz = cmdSz;
	}
	cmdData = lua_newuserdata( L, allocSz ); // + 1
	if (!cmdData) {
		return luaL_error(L, "Failed to allocate cmd memory");
	}
	memset( cmdData, 0, allocSz );
	cmdList = (void**) (((char*)cmdData) + (cmdSz * nArgB)); // point to the command list

	allocSz = resCSz + resEleSz * nArgB;
	resData = lua_newuserdata( L, allocSz); // + 1
	if (!resData) {
		return luaL_error(L, "Failed to allocate res memory");
	}
	memset( resData, 0, allocSz );
	
	luaL_getmetatable(L, lua_restype);	// +1
	lua_setmetatable(L, -2);			// -1 make the res object be the restype

	int cb_r = init_res_cb( L, resData, nArgB, &resElePtr );
	if (cb_r > 0) {
		return cb_r;
	}

	for (i = 0; i < nArgB; i++) { // iterate our batches of arguments
		cmdList[i] = ((char*)cmdData) + (cmdSz * i);
		cb_r = process_cmd_cb(L
							, i
							,cmdList[i]
							, 2 + (argBatchSz*i)
							,argBatchSz
							,(void*) ((char*)resElePtr + (resEleSz * i))
						);
		if (cb_r > 0) {
			return cb_r;
		}
	}
	cb_r = exec_op_cb(L,pInst,cmdData,cmdList,nArgB,resData);
	if (cb_r > 0) {
		return cb_r;
	}

	return 1; // return the response data structure
}
// OP DISPATCH  *************************************************************
// **************************************************************************


// #################################################################################
// GET ###########################
static int Get_InitRes( lua_State *L, void* pData, int eleCount, void** pEles) {
	GetRes* pRes = (GetRes*)pData;
	
	pRes->nentry	= eleCount;
	pRes->responded = 0;
	pRes->error[0]	= 0;
	pRes->entries	= (GetResEntry*) ( (char*)pData + sizeof(*pRes) );

	*pEles = pRes->entries;

	return 0;
}

static int Get_ProcessCmd( lua_State *L, int idx, void* cmdD, int argS, int argcount, void* resEleD )
{
	lcb_get_cmd_t	*pCur	= (lcb_get_cmd_t*)cmdD;
	GetResEntry		*pResE	= (GetResEntry*)resEleD;
	size_t			keysz;

	const char* keyname = luaL_checklstring(L, argS, &keysz);

	pResE->key = strndup(keyname, keysz);
	if (!pResE->key) {
		return luaL_error(L, "Failed to allocate memory for key response %s", keyname);
	}
	pResE->nkey = keysz;
	pCur->v.v0.key = pResE->key;
	pCur->v.v0.nkey = pResE->nkey;
	return 0;
}

static int Get_ExecOp( lua_State *L, Instance* pInst, void* pCmdD, void** pCmdList, size_t nCmds, void* pResD)
{
	lcb_error_t err = lcb_get( pInst->inst, pResD, nCmds, (const lcb_get_cmd_t* const*) pCmdList);
	if (err != LCB_SUCCESS) {
		lua_pushboolean( L, 0 );
		lua_pushfstring( L, "Error %s", lcb_strerror(NULL,err));
		return 2;
	}
	return 0;
}

static int Get( lua_State *L ) {
	return Op( L
				,sizeof(lcb_get_cmd_t)	,1
				,sizeof(GetRes)			,sizeof(GetResEntry),	GET_RESTYPE
				,Get_InitRes
				,Get_ProcessCmd
				,Get_ExecOp
			);
}
// GET ###########################
// #################################################################################

// #################################################################################
// STORE ###########################

static int Store_InitRes( lua_State *L, void* pData, int eleCount, void** pEles) {
	StoreRes* pRes = (StoreRes*)pData;

	pRes->nentry	= eleCount;
	pRes->entries	= (StoreResEntry*) ( ((char*)pData) + sizeof(*pRes) );

	*pEles = pRes->entries;
	return 0;
}

static int Store_ProcessCmd( lua_State *L, int idx, void* cmdD, int argS, int argcount, void* resEleD ) {
	lcb_store_cmd_t	*pCur = (lcb_store_cmd_t*)cmdD;
	StoreResEntry	*pRes = (StoreResEntry*)resEleD;

	const char* typeN		= NULL;

	const char* keyN		= NULL;
	size_t		keyNsz		= 0;

	const char* valueN		= NULL;
	size_t		valueNsz	= 0;

	int			flags		= 0;
	int			cas			= 0;
	int			timeout		= 0;

	typeN	= luaL_checkstring(L, argS);
	keyN	= luaL_checklstring(L, argS+1, &keyNsz);
	valueN	= luaL_checklstring(L, argS+2, &valueNsz);
	flags	= luaL_checkinteger(L, argS+3);
	cas		= luaL_checkinteger(L, argS+4);
	timeout	= luaL_checkinteger(L, argS+5);

	int op = LCB_ADD;
	if ( strncasecmp("ADD",typeN,3) == 0) {
		op = LCB_ADD;
	}
	else if ( strncasecmp("REP",typeN,3) == 0
			|| strncasecmp("CAS",typeN,3) == 0) {
		op = LCB_REPLACE;
	}
	else if ( strncasecmp("SET",typeN,3) == 0) {
		op = LCB_SET;
	}
	else if ( strncasecmp("APP",typeN,3) == 0) {
		op = LCB_APPEND;
	}
	else if ( strncasecmp("PRE",typeN,3) == 0) {
		op = LCB_PREPEND;
	}

	pRes->key	= strndup( keyN, keyNsz );
	pRes->op	= op;

	if (!pRes->key) {
		return luaL_error(L, "store cmd Failed to allocate memory for key response %s", keyN);
	}
	pRes->nkey				= keyNsz;
	pCur->v.v0.key			= pRes->key;
	pCur->v.v0.nkey			= keyNsz;
	pCur->v.v0.bytes		= valueN;
	pCur->v.v0.nbytes		= valueNsz;
	pCur->v.v0.flags		= flags;
	pCur->v.v0.cas			= cas;
	pCur->v.v0.exptime		= timeout;
	pCur->v.v0.operation	= op;

	return 0;
}

static int Store_ExecOp( lua_State *L, Instance* pInst, void* pCmdD, void** pCmdList, size_t nCmds, void* pResD) {
	lcb_error_t err = lcb_store( pInst->inst, pResD, nCmds, (const lcb_store_cmd_t* const*) pCmdList);
	if (err != LCB_SUCCESS) {
		lua_pushboolean( L, 0 );
		lua_pushfstring( L, "Error %s", lcb_strerror(NULL,err));
		return 2;
	}
	return 0;
}

static int Store( lua_State *L)
{
	return Op( L
				,sizeof(lcb_store_cmd_t)	,6 // TYPE,KEY,VALUE,FLAGS,CAS,TIMEOUT
				,sizeof(StoreRes)			,sizeof(StoreResEntry),	STORE_RESTYPE
				,Store_InitRes
				,Store_ProcessCmd
				,Store_ExecOp
			);
}
// STORE ###########################
// #################################################################################
// #################################################################################
// ARITHMATIC ###########################
static int Arith_InitRes( lua_State *L, void* pData, int eleCount, void** pEles) {
	ArithRes* pRes = (ArithRes*)pData;

	pRes->nentry	= eleCount;
	pRes->entries	= (ArithResEntry*) ( ((char*)pData) + sizeof(*pRes) );

	*pEles = pRes->entries;
	return 0;
}

static int Arith_ProcessCmd( lua_State *L, int idx, void* cmdD, int argS, int argcount, void* resEleD ) {
	lcb_arithmetic_cmd_t	*pCur = (lcb_arithmetic_cmd_t*)cmdD;
	ArithResEntry			*pRes = (ArithResEntry*)resEleD;

	const char* 	keyN		= NULL;
	size_t			keyNsz		= 0;
	lcb_int64_t		delta		= 0;
	lcb_time_t		timeout		= 0;
	lcb_uint64_t	initial		= 0;
	int				create		= 0;

	keyN	= luaL_checklstring(L, argS+0, &keyNsz);
	delta	= luaL_checkinteger(L, argS+1);
	timeout = luaL_checkint(L, argS+2);
	initial = luaL_checkint(L, argS+3);
	create	= luaL_checkint(L, argS+4);

	pRes->key	= strndup( keyN, keyNsz );
	pRes->nkey	= keyNsz;

	pCur->v.v0.key		= pRes->key;
	pCur->v.v0.nkey		= pRes->nkey;
	pCur->v.v0.exptime	= timeout;
	pCur->v.v0.create	= create;
	pCur->v.v0.delta	= delta;
	pCur->v.v0.initial	= initial;

	return 0;
}

static int Arith_ExecOp( lua_State *L, Instance* pInst, void* pCmdD, void** pCmdList, size_t nCmds, void* pResD) {
	lcb_error_t err =  lcb_arithmetic( pInst->inst, pResD, nCmds, (const lcb_arithmetic_cmd_t* const*) pCmdList);
	if (err != LCB_SUCCESS) {
		lua_pushboolean( L, 0 );
		lua_pushfstring( L, "Error %s", lcb_strerror(NULL,err));
		return 2;
	}
	return 0;
}

static int Arith(lua_State *L) {
	return Op( L
				,sizeof(lcb_arithmetic_cmd_t)	,5 // KEY,DELTA,TIMEO,INITAL,CREATE
				,sizeof(ArithRes)				,sizeof(ArithResEntry), ARITH_RESTYPE
				,Arith_InitRes
				,Arith_ProcessCmd
				,Arith_ExecOp
			);
}

// ARITHMATIC ###########################
// #################################################################################
// #################################################################################
// REMOVE ###########################
static int Remove_InitRes( lua_State *L, void* pData, int eleCount, void** pEles) {
	RemoveRes* pRes = (RemoveRes*)pData;

	pRes->nentry	= eleCount;
	pRes->entries	= (RemoveResEntry*) ( ((char*)pData) + sizeof(*pRes) );

	*pEles = pRes->entries;
	return 0;
}

static int Remove_ProcessCmd( lua_State *L, int idx, void* cmdD, int argS, int argcount, void* resEleD ) {
	lcb_remove_cmd_t	*pCur = (lcb_remove_cmd_t*)cmdD;
	RemoveResEntry		*pRes = (RemoveResEntry*)resEleD;

	const char* 	keyN		= NULL;
	size_t			keyNsz		= 0;
	lcb_cas_t		cas			= 0;

	keyN	= luaL_checklstring(L, argS+0, &keyNsz);
	cas		= luaL_checkinteger(L, argS+1);

	pRes->key	= strndup( keyN, keyNsz );
	pRes->nkey	= keyNsz;

	pCur->v.v0.key		= pRes->key;
	pCur->v.v0.nkey		= pRes->nkey;
	pCur->v.v0.cas		= cas;

	return 0;
}

static int Remove_ExecOp( lua_State *L, Instance* pInst, void* pCmdD, void** pCmdList, size_t nCmds, void* pResD) {
	lcb_error_t err =  lcb_remove( pInst->inst, pResD, nCmds, (const lcb_remove_cmd_t* const*) pCmdList);
	if (err != LCB_SUCCESS) {
		lua_pushboolean( L, 0 );
		lua_pushfstring( L, "Error %s", lcb_strerror(NULL,err));
		return 2;
	}
	return 0;
}

static int Remove(lua_State *L) {
	return Op( L
				,sizeof(lcb_remove_cmd_t)	,2 // KEY,CAS
				,sizeof(RemoveRes)				,sizeof(RemoveResEntry), REMOVE_RESTYPE
				,Remove_InitRes
				,Remove_ProcessCmd
				,Remove_ExecOp
			);
}
// REMOVE ###########################
// #################################################################################
// #################################################################################
// FLUSH ###########################
static int Flush_InitRes( lua_State *L, void* pData, int eleCount, void** pEles) {
	FlushRes* pRes = (FlushRes*)pData;

	pRes->nentry	= eleCount;
	pRes->entries	= (FlushResEntry*) ( ((char*)pData) + sizeof(*pRes) );

	*pEles = pRes->entries;
	return 0;
}

static int Flush_ProcessCmd( lua_State *L, int idx, void* cmdD, int argS, int argcount, void* resEleD ) {
	return 0;
}

static int Flush_ExecOp( lua_State *L, Instance* pInst, void* pCmdD, void** pCmdList, size_t nCmds, void* pResD) {
	lcb_error_t err =  lcb_flush( pInst->inst, pResD, nCmds, (const lcb_flush_cmd_t* const*) pCmdList);
	if (err != LCB_SUCCESS) {
		lua_pushboolean( L, 0 );
		lua_pushfstring( L, "Error %s", lcb_strerror(NULL,err));
		return 2;
	}
	return 0;
}

static int Flush(lua_State *L) {
	return Op( L
				,sizeof(lcb_flush_cmd_t)	,0
				,sizeof(FlushRes)				,sizeof(FlushResEntry), REMOVE_RESTYPE
				,Flush_InitRes
				,Flush_ProcessCmd
				,Flush_ExecOp
			);
}
// FLUSH ###########################
// #################################################################################


static int Touch(lua_State *L) {
}

static int Stats(lua_State *L) {
}

static int Wait(lua_State *L) {
	lcb_error_t	err;
	Instance *pinstance;
	pinstance = (Instance*)luaL_checkudata(L, 1, TYPENAME);
	
	if (!pinstance || !pinstance->valid) {
		return luaL_error(L, "Invalid instance data");	
	}

	err = lcb_wait( pinstance->inst );
	if ( err != LCB_SUCCESS ) {
		lua_pushboolean( L, 0 );										// +1
		lua_pushfstring( L, "Error %s", lcb_strerror(NULL,err)); // +1
		return 2;
	}
	lua_pushboolean( L, 1 );										// +1
	return 1;
}

// connect( host:port, bucket, user /* = NULL */, pass /* = NULL */ )
//
static int Connect(lua_State *L) {
	size_t specSz	= 0;
	size_t bucketSz	= 0;
	size_t userSz	= 0;
	size_t passSz	= 0;

	const char* specStr		= NULL;
	const char* bucketStr	= NULL;
	const char* userStr		= NULL;
	const char* passStr		= NULL;

	int			top			= 0;
	Instance	*pinstance	= NULL;
	lcb_error_t	err;

	struct lcb_create_io_ops_st io_opts;
	struct lcb_create_st create_options;
	memset(&create_options, 0, sizeof(create_options));

	io_opts.version		= 0;
	io_opts.v.v0.type	= LCB_IO_OPS_DEFAULT;
	io_opts.v.v0.cookie	= NULL;
	err = lcb_create_io_ops(&create_options.v.v0.io, &io_opts);
	if (err != LCB_SUCCESS) {
		lua_pushboolean( L, 0 );											// +1
		lua_pushfstring( L, "IO Ops failure %s", lcb_strerror(NULL,err));	// +1
		return 2;
	}

	top = lua_gettop(L);

	specStr		= luaL_checklstring(L, 1, &specSz);		// host:port
	bucketStr	= luaL_checklstring(L, 2, &bucketSz);	// bucket
	if (top > 2) {
		userStr = luaL_checklstring(L, 3, &userSz);
	}
	if (top> 3 ) {
		passStr = luaL_checklstring(L, 4, &passSz);
	}

	create_options.v.v0.host	= specStr;
	create_options.v.v0.user	= userStr;
	create_options.v.v0.passwd	= passStr;
	create_options.v.v0.bucket	= bucketStr;

	pinstance = lua_newuserdata( L, sizeof(*pinstance) );				// +1
	if (!pinstance) {
		return luaL_error(L, "Failed to allocate instance pointer");	
	}
	pinstance->valid = 0;

	luaL_getmetatable(L, TYPENAME);								// +1
	lua_setmetatable(L, -2);									// -1

	err = lcb_create( &pinstance->inst, &create_options );
	if ( err != LCB_SUCCESS ) {
		lua_pushboolean( L, 0 );										// +1
		lua_pushfstring( L, "Connection CREATE failure %s", lcb_strerror(NULL,err)); // +1
		return 2;
	}
    
	(void)lcb_set_error_callback		( pinstance->inst, error_cb		);

	err = lcb_connect( pinstance->inst );
	if (err != LCB_SUCCESS) {
		lua_pushboolean( L, 0 );										// +1
		lua_pushfstring( L, "Connection CONNECT failure %s", lcb_strerror(NULL,err)); // +1
		return 2;
	}
	
	(void)lcb_set_get_callback			( pinstance->inst, get_cb		);
	(void)lcb_set_touch_callback		( pinstance->inst, touch_cb		);
	(void)lcb_set_store_callback		( pinstance->inst, store_cb		);
    (void)lcb_set_arithmetic_callback	( pinstance->inst, arith_cb		);
    (void)lcb_set_remove_callback		( pinstance->inst, remove_cb	);
    (void)lcb_set_stat_callback			( pinstance->inst, stat_cb		);
    (void)lcb_set_touch_callback		( pinstance->inst, touch_cb		);
    (void)lcb_set_flush_callback		( pinstance->inst, flush_cb		);

	err = lcb_wait( pinstance->inst );
	if ( err != LCB_SUCCESS ) {
		lua_pushboolean( L, 0 );										// +1
		lua_pushfstring( L, "Error %s", lcb_strerror(NULL,err)); // +1
		return 2;
	}

	pinstance->valid = 1;
	return 1; // return pinstance
}

static int __gc(lua_State *L) {
	Instance *pinstance	= NULL;
	pinstance = (Instance*)luaL_checkudata(L, 1, TYPENAME);

	if (pinstance->valid) {
		pinstance->valid = 0;
		lcb_destroy( pinstance->inst );
	}
	return 0;	
}

// ------------------------------------------------
// ----------------- GET

static int __gc_get_res(lua_State *L) {
	int		i		= 0;
	GetRes *pres	= NULL;

	pres = (GetRes*)luaL_checkudata(L, 1, GET_RESTYPE);

	for (i = 0; i < pres->nentry; i++) {
		GetResEntry* entry = &pres->entries[i];
		if (entry->key) {
			free(entry->key);
			entry->key = NULL;
			entry->nkey = 0;
		}
		if (entry->data) {
			free(entry->data);
			entry->data = NULL;
			entry->ndata = 0;
		}
	}
	return 0;
}
	
static int __res_get_isfinished(lua_State *L) {
	GetRes *pres;
	pres = (GetRes*)luaL_checkudata(L, 1, GET_RESTYPE);
	lua_pushboolean( L, pres->responded == pres->nentry );
	return 1;
}

static int __res_get_keycount(lua_State *L) {
	GetRes *pres	= NULL;
	pres = (GetRes*)luaL_checkudata(L, 1, GET_RESTYPE);
	lua_pushinteger(L, pres->nentry);
	return 1;
}

static int __res_get_errorstr(lua_State *L) {
	GetRes *pres	= NULL;
	pres = (GetRes*)luaL_checkudata(L, 1, GET_RESTYPE);
	lua_pushstring(L, pres->error);
	return 1;
}

static int __res_get_iterate(lua_State *L) {
	int		top		= 0;
	int		i		= 0;
	int		q		= 0;
	GetRes *pres	= NULL;

	pres = (GetRes*)luaL_checkudata(L, 1, GET_RESTYPE);

	top = lua_gettop(L);
	if (top < 2) {
		lua_pushboolean( L, 0 );										// +1
		lua_pushfstring( L, "Invalid call; fn(key,val,flags,cas,error,responded,...)" ); // +1
		return 2;
	}

	// assume stack 2 + is the functon and passthrough arguments

	for (i = 0; i < pres->nentry; i++) {
		int p = 0;
		lua_Integer test;

		lua_pushvalue(L, 2); // repush the fn
		GetResEntry* entry = &pres->entries[i];
		lua_pushlstring(L, entry->key, entry->nkey);	p++;
		if (entry->ndata > 0 ) {
			lua_pushlstring(L, entry->data, entry->ndata);	p++;
		}
		else {
			lua_pushnil(L); p++;
		}
		lua_pushinteger(L, entry->flags);		p++;
		lua_pushinteger(L, entry->cas);			p++;
		lua_pushstring(L, entry->error);		p++;
		lua_pushboolean(L, entry->responded);	p++;
		for (q = 3; q <= top; q++) { // repush args
			p++;
			lua_pushvalue(L,q);
		}
		lua_call(L, p, 1);
		test = lua_tointeger(L,-1);
		lua_pop(L,1);
		if (test > 0) {
			break;
		}
	}
	lua_pushboolean(L,1);
	lua_pushinteger(L,i);
	return 2;
}
// ----------------- GET
// ------------------------------------------------

// ------------------------------------------------
// ----------------- STORE
static int __gc_store_res(lua_State *L) {
	int i = 0;
	StoreRes *pRes = (StoreRes*)luaL_checkudata(L, 1, STORE_RESTYPE);
	
	for (i = 0; i < pRes->nentry; i++) {
		StoreResEntry* entry = &pRes->entries[i];
		if (entry->key) {
			free(entry->key);
			entry->key = NULL;
			entry->nkey = 0;
		}
	}
	return 0;
}

static int __res_store_isfinished(lua_State *L) {
	StoreRes *pRes = (StoreRes*)luaL_checkudata(L, 1, STORE_RESTYPE);
	lua_pushboolean(L, pRes->nentry == pRes->responded );
	return 1;
}

static int __res_store_iterate(lua_State *L) {
	int i = 0, top = 0;
	StoreRes *pRes = (StoreRes*)luaL_checkudata(L, 1, STORE_RESTYPE);
	
	top = lua_gettop(L);
	if (top < 2) {
		lua_pushboolean( L, 0 );										// +1
		lua_pushfstring( L, "Invalid call, need fn(key,cas,op,error,responded,...)" ); // +1
		return 2;
	}

	for (i = 0; i < pRes->nentry; i++) {
		StoreResEntry* entry = &pRes->entries[i];
		
		int p = 0;
		int q = 0;
		lua_Integer test;

		lua_pushvalue(L, 2); // repush the fn
		lua_pushlstring(L, entry->key, entry->nkey);p++;
		lua_pushinteger(L, entry->cas);			p++;
		lua_pushinteger(L, entry->op);			p++;
		lua_pushstring(L, entry->error);		p++;
		lua_pushboolean(L, entry->responded);	p++;
		for (q = 3; q <= top; q++) { // repush args
			p++;
			lua_pushvalue(L,q);
		}
		lua_call(L, p, 1);
		test = lua_tointeger(L,-1);
		lua_pop(L,1);
		if (test > 0) {
			break;
		}
	}
	lua_pushboolean(L,1);
	lua_pushinteger(L,i);
	return 2;
}

static int __res_store_keycount(lua_State *L) {
	StoreRes *pRes = (StoreRes*)luaL_checkudata(L, 1, STORE_RESTYPE);
	lua_pushinteger(L, pRes->nentry);
	return 1;
}
static int __res_store_errorstr(lua_State *L) {
	StoreRes *pRes = (StoreRes*)luaL_checkudata(L, 1, STORE_RESTYPE);
	lua_pushstring(L, pRes->error);
	return 1;
}
// ----------------- STORE
// ------------------------------------------------
// ------------------------------------------------
// ----------------- ARITH
static int __gc_arith_res(lua_State *L) {
	int i = 0;
	ArithRes *pRes = (ArithRes*)luaL_checkudata(L, 1, ARITH_RESTYPE);
	
	for (i = 0; i < pRes->nentry; i++) {
		ArithResEntry* entry = &pRes->entries[i];
		if (entry->key) {
			free(entry->key);
			entry->key = NULL;
			entry->nkey = 0;
		}
	}
	return 0;
}

static int __res_arith_isfinished(lua_State *L) {
	ArithRes *pRes = (ArithRes*)luaL_checkudata(L, 1, ARITH_RESTYPE);
	lua_pushboolean(L, pRes->nentry == pRes->responded );
	return 1;
}

static int __res_arith_iterate(lua_State *L) {
	int i = 0, top = 0;
	ArithRes *pRes = (ArithRes*)luaL_checkudata(L, 1, ARITH_RESTYPE);
	
	top = lua_gettop(L);
	if (top < 2) {
		lua_pushboolean( L, 0 );										// +1
		lua_pushstring( L, "Invalid call, need fn(key,value,cas,error,responded,...)" ); // +1
		return 2;
	}

	for (i = 0; i < pRes->nentry; i++) {
		ArithResEntry* entry = &pRes->entries[i];
		
		int p = 0;
		int q = 0;
		lua_Integer test;

		lua_pushvalue(L, 2); // repush the fn
		lua_pushlstring(L, entry->key, entry->nkey);p++;
		lua_pushinteger(L, entry->value);		p++;
		lua_pushinteger(L, entry->cas);			p++;
		lua_pushstring(L, entry->error);		p++;
		lua_pushboolean(L, entry->responded);	p++;
		for (q = 3; q <= top; q++) { // repush args
			p++;
			lua_pushvalue(L,q);
		}
		lua_call(L, p, 1);
		test = lua_tointeger(L,-1);
		lua_pop(L,1);
		if (test > 0) {
			break;
		}
	}
	lua_pushboolean(L,1);
	lua_pushinteger(L,i);
	return 2;
}

static int __res_arith_keycount(lua_State *L) {
	ArithRes *pRes = (ArithRes*)luaL_checkudata(L, 1, ARITH_RESTYPE);
	lua_pushinteger(L, pRes->nentry);
	return 1;
}
static int __res_arith_errorstr(lua_State *L) {
	ArithRes *pRes = (ArithRes*)luaL_checkudata(L, 1, ARITH_RESTYPE);
	lua_pushstring(L, pRes->error);
	return 1;
}

// ----------------- ARITH
// ------------------------------------------------
// ------------------------------------------------
// ----------------- REMOVE
static int __gc_remove_res(lua_State *L) {
	int i = 0;
	RemoveRes *pRes = (RemoveRes*)luaL_checkudata(L, 1, REMOVE_RESTYPE);
	
	for (i = 0; i < pRes->nentry; i++) {
		RemoveResEntry* entry = &pRes->entries[i];
		if (entry->key) {
			free(entry->key);
			entry->key = NULL;
			entry->nkey = 0;
		}
	}
	return 0;
}

static int __res_remove_isfinished(lua_State *L) {
	RemoveRes *pRes = (RemoveRes*)luaL_checkudata(L, 1, REMOVE_RESTYPE);
	lua_pushboolean(L, pRes->nentry == pRes->responded );
	return 1;
}

static int __res_remove_iterate(lua_State *L) {
	int i = 0, top = 0;
	RemoveRes *pRes = (RemoveRes*)luaL_checkudata(L, 1, REMOVE_RESTYPE);
	
	top = lua_gettop(L);
	if (top < 2) {
		lua_pushboolean( L, 0 );										// +1
		lua_pushfstring( L, "Invalid call, need fn(key,cas,error,responded,...)" ); // +1
		return 2;
	}

	for (i = 0; i < pRes->nentry; i++) {
		RemoveResEntry* entry = &pRes->entries[i];
		
		int p = 0;
		int q = 0;
		lua_Integer test;

		lua_pushvalue(L, 2); // repush the fn
		lua_pushlstring(L, entry->key,entry->nkey);	p++;
		lua_pushinteger(L, entry->cas);			p++;
		lua_pushstring(L, entry->error);		p++;
		lua_pushboolean(L, entry->responded);	p++;
		for (q = 3; q <= top; q++) { // repush args
			p++;
			lua_pushvalue(L,q);
		}
		lua_call(L, p, 1);
		test = lua_tointeger(L,-1);
		lua_pop(L,1);
		if (test > 0) {
			break;
		}
	}
	lua_pushboolean(L,1);
	lua_pushinteger(L,i);
	return 2;
}

static int __res_remove_keycount(lua_State *L) {
	RemoveRes *pRes = (RemoveRes*)luaL_checkudata(L, 1, REMOVE_RESTYPE);
	lua_pushinteger(L, pRes->nentry);
	return 1;
}
static int __res_remove_errorstr(lua_State *L) {
	RemoveRes *pRes = (RemoveRes*)luaL_checkudata(L, 1, REMOVE_RESTYPE);
	lua_pushstring(L, pRes->error);
	return 1;
}

// ----------------- REMOVE
// ------------------------------------------------
// ------------------------------------------------
// ----------------- FLUSH
static int __gc_flush_res(lua_State *L) {
	return 0;
}

static int __res_flush_isfinished(lua_State *L) {
	FlushRes *pRes = (FlushRes*)luaL_checkudata(L, 1, FLUSH_RESTYPE);
	lua_pushboolean(L, pRes->responded > 0 );
	return 1;
}

static int __res_flush_errorstr(lua_State *L) {
	RemoveRes *pRes = (RemoveRes*)luaL_checkudata(L, 1, FLUSH_RESTYPE);
	lua_pushstring(L, pRes->error);
	return 1;
}
// ----------------- FLUSH
// ------------------------------------------------

static const struct luaL_reg luaLibCBase[] = {
	{ "connect", Connect },	// connect to a couchbase cluster and returning light user data for the connection
	{ "wait", Wait },
	{ "get", Get },
	{ "store", Store },
	{ "touch", Touch },
	{ "arith", Arith },
	{ "remove", Remove },
	{ "stats", Stats },
	{ "flush", Flush },
	{ NULL, NULL },
};

static int setupGet(lua_State* L) {
	luaL_newmetatable(L, GET_RESTYPE);  // + 1
	lua_pushliteral(L, "__index");
	lua_pushvalue(L, -2); 
	lua_settable(L,-3);
	lua_pushliteral(L, "__gc");
	lua_pushcfunction(L, __gc_get_res);
	lua_settable(L,-3);
	lua_pushliteral(L, "isFinished");
	lua_pushcfunction(L, __res_get_isfinished);
	lua_settable(L,-3);
	lua_pushliteral(L, "iterateRes");
	lua_pushcfunction(L, __res_get_iterate);
	lua_settable(L,-3);
	lua_pushliteral(L, "keyCount");
	lua_pushcfunction(L, __res_get_keycount);
	lua_settable(L,-3);
	lua_pushliteral(L, "errorStr");
	lua_pushcfunction(L, __res_get_errorstr);
	lua_settable(L,-3);
	lua_pop(L,1);
}

static int setupStore(lua_State* L) {
	luaL_newmetatable(L, STORE_RESTYPE);  // + 1
	lua_pushliteral(L, "__index");
	lua_pushvalue(L, -2); 
	lua_settable(L,-3);
	lua_pushliteral(L, "__gc");
	lua_pushcfunction(L, __gc_store_res);
	lua_settable(L,-3);
	lua_pushliteral(L, "isFinished");
	lua_pushcfunction(L, __res_store_isfinished);
	lua_settable(L,-3);
	lua_pushliteral(L, "iterateRes");
	lua_pushcfunction(L, __res_store_iterate);
	lua_settable(L,-3);
	lua_pushliteral(L, "keyCount");
	lua_pushcfunction(L, __res_store_keycount);
	lua_settable(L,-3);
	lua_pushliteral(L, "errorStr");
	lua_pushcfunction(L, __res_store_errorstr);
	lua_settable(L,-3);
	lua_pop(L,1);
}

static int setupArith(lua_State* L) {
	luaL_newmetatable(L, ARITH_RESTYPE);  // + 1
	lua_pushliteral(L, "__index");
	lua_pushvalue(L, -2); 
	lua_settable(L,-3);
	lua_pushliteral(L, "__gc");
	lua_pushcfunction(L, __gc_arith_res);
	lua_settable(L,-3);
	lua_pushliteral(L, "isFinished");
	lua_pushcfunction(L, __res_arith_isfinished);
	lua_settable(L,-3);
	lua_pushliteral(L, "iterateRes");
	lua_pushcfunction(L, __res_arith_iterate);
	lua_settable(L,-3);
	lua_pushliteral(L, "keyCount");
	lua_pushcfunction(L, __res_arith_keycount);
	lua_settable(L,-3);
	lua_pushliteral(L, "errorStr");
	lua_pushcfunction(L, __res_arith_errorstr);
	lua_settable(L,-3);
	lua_pop(L,1);
}

static int setupRemove(lua_State* L) {
	luaL_newmetatable(L, REMOVE_RESTYPE);  // + 1
	lua_pushliteral(L, "__index");
	lua_pushvalue(L, -2); 
	lua_settable(L,-3);
	lua_pushliteral(L, "__gc");
	lua_pushcfunction(L, __gc_remove_res);
	lua_settable(L,-3);
	lua_pushliteral(L, "isFinished");
	lua_pushcfunction(L, __res_remove_isfinished);
	lua_settable(L,-3);
	lua_pushliteral(L, "iterateRes");
	lua_pushcfunction(L, __res_remove_iterate);
	lua_settable(L,-3);
	lua_pushliteral(L, "keyCount");
	lua_pushcfunction(L, __res_remove_keycount);
	lua_settable(L,-3);
	lua_pushliteral(L, "errorStr");
	lua_pushcfunction(L, __res_remove_errorstr);
	lua_settable(L,-3);
	lua_pop(L,1);
}

static int setupFlush(lua_State* L) {
	luaL_newmetatable(L, FLUSH_RESTYPE);  // + 1
	lua_pushliteral(L, "__index");
	lua_pushvalue(L, -2); 
	lua_settable(L,-3);
	lua_pushliteral(L, "__gc");
	lua_pushcfunction(L, __gc_flush_res);
	lua_settable(L,-3);
	lua_pushliteral(L, "isFinished");
	lua_pushcfunction(L, __res_flush_isfinished);
	lua_settable(L,-3);
	lua_pushliteral(L, "errorStr");
	lua_pushcfunction(L, __res_flush_errorstr);
	lua_settable(L,-3);
	lua_pop(L,1);
}


LUA_API int luaopen_libcbase(lua_State *L) {
	luaL_newmetatable(L, TYPENAME); // + 1
	lua_pushliteral(L, "__gc");      // + 1
	lua_pushcfunction(L, __gc);     // + 1
	lua_settable(L,-3);             // -2

	setupGet(L);
	setupStore(L);
	setupArith(L);
	setupRemove(L);
	setupFlush(L);

	luaL_register(L, "libcbase", luaLibCBase);

	return 1;
}
/*
int main(char *argc[], int argv) { 
	EvalHand(NULL);
}
*/
