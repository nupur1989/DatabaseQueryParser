/********************************************************************
db.h - This file contains all the structures, defines, and function
	prototype for the db.exe program.
*********************************************************************/

#define MAX_IDENT_LEN   16
#define MAX_NUM_COL			16
#define MAX_TOK_LEN			32
#define KEYWORD_OFFSET	10
#define STRING_BREAK		" (),<>="
#define NUMBER_BREAK		" ),"

#define ROLLFORWARD_PENDING   1

/* Column descriptor sturcture = 20+4+4+4+4 = 36 bytes */
typedef struct cd_entry_def
{
	char		col_name[MAX_IDENT_LEN+4];
	int			col_id;                   /* Start from 0 */
	int			col_type;
	int			col_len;
	int 		not_null;
} cd_entry;

/* Table packed descriptor sturcture = 4+20+4+4+4 = 36 bytes
   Minimum of 1 column in a table - therefore minimum size of
	 1 valid tpd_entry is 36+36 = 72 bytes. */
typedef struct tpd_entry_def
{
	int				tpd_size;
	char			table_name[MAX_IDENT_LEN+4];
	int				num_columns;
	int				cd_offset;
	int       tpd_flags;
} tpd_entry;

/* Table packed descriptor list = 4+4+4+36 = 48 bytes.  When no
   table is defined the tpd_list is 48 bytes.  When there is 
	 at least 1 table, then the tpd_entry (36 bytes) will be
	 overlapped by the first valid tpd_entry. */

typedef struct tpd_list_def
{
	int				list_size;
	int				num_tables;
	int				db_flags;
	tpd_entry	tpd_start;
}tpd_list;

/* This token_list definition is used for breaking the command
   string into separate tokens in function get_tokens().  For
	 each token, a new token_list will be allocated and linked 
	 together. */
typedef struct t_list
{
	char	tok_string[MAX_TOK_LEN];
	int		tok_class;
	int		tok_value;
	struct t_list *next;
} token_list;

typedef struct table_file_header_def 
{
	int file_size;
	int record_size;
	int num_records;
	int record_offset;
	int file_header_flag;
	tpd_entry *tpd_ptr;
}table_file_header;

typedef struct record_ne
{
	char col_len;
	char record_string[MAX_TOK_LEN];
	int cType;
	int max_length;
	struct record_ne *next;
	
}record_new_entry;


typedef struct column_list_def
{
	char col_name[MAX_TOK_LEN];
	struct column_list_def *next;

}column_list;


typedef struct where_list_def
{
	char col_name[MAX_TOK_LEN];
	int null_check ;
	int arith_operator;	
	char value[MAX_TOK_LEN];
	int col_type;	
	int relational_operator;
	
	struct where_list_def *next;

}where_list;


/* This enum defines the different classes of tokens for 
	 semantic processing. */
typedef enum t_class
{
	keyword = 1,	// 1
	identifier,		// 2
	symbol, 			// 3
	type_name,		// 4
	constant,		  // 5
  function_name,// 6
	terminator,		// 7
	error			    // 8
  
} token_class;

/* This enum defines the different values associated with
   a single valid token.  Use for semantic processing. */
typedef enum t_value
{
	T_INT = 10,		// 10 - new type should be added above this line
	T_CHAR,		    // 11 
	K_CREATE, 		// 12
	K_TABLE,			// 13
	K_NOT,				// 14
	K_NULL,				// 15
	K_DROP,				// 16
	K_LIST,				// 17
	K_SCHEMA,			// 18
  K_FOR,        // 19
	K_TO,				  // 20
  K_INSERT,     // 21
  K_INTO,       // 22
  K_VALUES,     // 23
  K_DELETE,     // 24
  K_FROM,       // 25
  K_WHERE,      // 26
  K_UPDATE,     // 27
  K_SET,        // 28
  K_SELECT,     // 29
  K_ORDER,      // 30
  K_BY,         // 31
  K_DESC,       // 32
  K_IS,         // 33
  K_AND,        // 34
  K_OR,         // 35 - new keyword should be added below this line
  K_BACKUP,
  K_RESTORE,
  K_WITHOUT,
  K_RF,
  K_ROLLFORWARD,
  F_SUM,        // 36
  F_AVG,        // 37
	F_COUNT,      // 38 - new function name should be added below this line
	S_LEFT_PAREN = 70,  // 70
	S_RIGHT_PAREN,		  // 71
	S_COMMA,			      // 72
  S_STAR,             // 73
  S_EQUAL,            // 74
  S_LESS,             // 75
  S_GREATER,          // 76
	IDENT = 85,			    // 85
	INT_LITERAL = 90,	  // 90
  STRING_LITERAL,     // 91
	EOC = 95,			      // 95
	INVALID = 99, // 99
} token_value;

/* This constants must be updated when add new keywords */
#define TOTAL_KEYWORDS_PLUS_TYPE_NAMES 34

/* New keyword must be added in the same position/order as the enum
   definition above, otherwise the lookup will be wrong */
char *keyword_table[] = 
{
  "int", "char", "create", "table", "not", "null", "drop", "list", "schema",
  "for", "to", "insert", "into", "values", "delete", "from", "where", 
  "update", "set", "select", "order", "by", "desc", "is", "and", "or", "backup","restore","without","rf","rollforward",
  "sum", "avg", "count"
};

/* This enum defines a set of possible statements */
typedef enum s_statement
{
  INVALID_STATEMENT = -199,	// -199
	CREATE_TABLE = 100,				// 100
	DROP_TABLE,								// 101
	LIST_TABLE,								// 102
	LIST_SCHEMA,							// 103
  INSERT,                   // 104
  DELETE,                   // 105
  UPDATE,                   // 106
  SELECT,                   // 107
  BACKUP,
  RESTORE,
  ROLLFORWARD
} semantic_statement;

/* This enum has a list of all the errors that should be detected
   by the program.  Can append to this if necessary. */
typedef enum error_return_codes
{
	INVALID_TABLE_NAME = -399,	// -399
	DUPLICATE_TABLE_NAME,				// -398
	TABLE_NOT_EXIST,						// -397
	INVALID_TABLE_DEFINITION,		// -396
	INVALID_COLUMN_NAME,				// -395
	DUPLICATE_COLUMN_NAME,			// -394
	COLUMN_NOT_EXIST,						// -393
	MAX_COLUMN_EXCEEDED,				// -392
	INVALID_TYPE_NAME,					// -391
	INVALID_COLUMN_DEFINITION,	// -390
	INVALID_COLUMN_LENGTH,			// -389
  INVALID_REPORT_FILE_NAME,		// -388
  /* Must add all the possible errors from I/U/D + SELECT here */
	FILE_OPEN_ERROR = -299,			// -299
	DBFILE_CORRUPTION,					// -298
	MEMORY_ERROR,							  // -297
//nupur
    INVALID_INSERT_DEFINITION, //-296
	INVALID_NOT_NULL_CONSTRAINT, //-295
	DATATYPE_MISMATCH_ERROR,//-294
	COMPLETE_COL_NOT_DEFINED, //-293
	INVALID_SELECT_STATEMENT,  //-292
	INVALID_UPDATE_STATEMENT,//-291
	COL_NOT_EXIST,     //-290
	EQUAL_NOT_PRESENT, //-289
	INVALID_DELETE_STATEMENT, //-288
	MAX_LENGTH_EXCEEDED, //-287
    INVALID_WHERE_CLAUSE, //-286
	INVALID_AGGREGATE_FUNCT, //-285
	INVALID_ORDER_BY, //-284
	INVALID_SYMBOL, //-283
	INVALID_BACKUP,
	INVALID_RESTORE,
	DUPLICATE_BACKUP,
	INVALID_ROLLFORWARD,

} return_codes;

/* Set of function prototypes */
int get_token(char *command, token_list **tok_list);
void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value);
int do_semantic(token_list *tok_list);
int sem_create_table(token_list *t_list);
int sem_drop_table(token_list *t_list);
int sem_list_tables();
int sem_list_schema(token_list *t_list);

//nupur
int sem_insert_table(token_list *t_list);
void add_to_recordList(record_new_entry **record_list , char *value, int col_len ,int type );
int add_record_to_file(record_new_entry *rec_entry, char *tab);

//select
int sem_select(token_list *t_list);
int select_from_file(char *tab);
int get_col_from_table( char *col, char *tab);
int get_colType_from_table( char *col, char *tab);
void add_to_colList(column_list **col_list ,char *value);
int select_col_from_file(column_list *col_list ,char*tab);
int select_all_where_clause(where_list *where_col_list,char *tab);
int select_col_where_clause(where_list *where_col_list, char *tab , column_list *col_list);
int select_aggregate(where_list *where_col_list, char *tab ,int star,int aggregate,char* col_name );

int select_order_by(where_list *where_col_list,column_list *col_list, char *tab, char *order_by_col_name, int col_type,int desc);
int* create_where_list(where_list *where_col_list, char *tab ,int *p ,int del_call,int *a);

//update
int sem_update_table(token_list *t_list);
int update_file(char *tab ,char *col_name, char *value);

//delete
int sem_delete_table(token_list *t_list);
int delete_from_file(char *tab);
int delete_from_file_where_clause(where_list *where_col_list ,char *tab);

//select_where
void add_to_where_list(where_list **where_col_list ,char *col_name, int arith_operator ,char *value , int flag ,int rel_op,int col_type);


//update_where
int update_where_clause(where_list *where_col_list, char *tab, char*col_name, char* value);

//backup
int sem_backup_table(token_list *t_list);

//restore
int sem_restore_from_table( token_list *t_list);
int restore(int rf_flag, char *file_name);

//rollforward
int sem_rollforward(token_list *t_list);
int rollforward(int time_flag, char *timestamp);
int execute(char *query);


/*
	Keep a global list of tpd - in real life, this will be stored
	in shared memory.  Build a set of functions/methods around this.
*/
tpd_list	*g_tpd_list;

int initialize_tpd_list();
int add_tpd_to_list(tpd_entry *tpd);
int drop_tpd_from_list(char *tabname);
tpd_entry* get_tpd_from_list(char *tabname);

int log = 0; //flag for logging command
