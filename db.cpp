/************************************************************
Project#1:	CLP & DDL
************************************************************/

#include "db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <io.h>

int main(int argc, char** argv)
{

	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;

	printf("%d\n",argc);
	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"");
		return 1;
	}


	rc = initialize_tpd_list();


	if (rc)
	{
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
	}
	else
	{
		rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
				tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}

		if (!rc)
		{
			rc = do_semantic(tok_list);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					(tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

		if(rc == 0 && log == 1)
		{
			
			time_t t = time(0);
			struct tm* lt = localtime(&t);
			char time_str[15];
			sprintf(time_str, "%04d%02d%02d%02d%02d%02d",lt->tm_year + 1900, lt->tm_mon + 1, lt->tm_mday,lt->tm_hour, lt->tm_min, lt->tm_sec);
			//printf("%s",time_str);
			printf("Transaction LOG\n");
			FILE *fl;

			//time_str[strlen(time_str)] = '\0';
			strcat(argv[1],"\"\n");

			fl = fopen("db.log", "a");

			if(fl == NULL)
			{
				printf("Could not open File:db.log\n");					
			}
			else
			{
				printf("Writing into the Transaction file\n");
				fwrite(time_str,1,strlen(time_str),fl);
				fwrite(" \"",2*(sizeof(char)),1,fl);
				fwrite(argv[1],1,strlen(argv[1]),fl);		
				fflush(fl);
			    fclose(fl);
			}		

		}

		/* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			tmp_tok_ptr = tok_ptr->next;
			free(tok_ptr);
			tok_ptr=tmp_tok_ptr;
		}
	}

	return rc;
}

/************************************************************* 
This is a lexical analyzer for simple SQL statements
*************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;

	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
		memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				is not a blank, (, ), or a comma, then append this
				character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((stricmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				//printf("F:K--%d",found_keyword);
				if (found_keyword)
				{
					if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
						t_class = function_name;
					else
						t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
						add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do 
			{
				temp_string[i++] = *cur++;
			}
			while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				is not a blank or a ), then append this
				character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
			|| (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
			case '(' : t_value = S_LEFT_PAREN; break;
			case ')' : t_value = S_RIGHT_PAREN; break;
			case ',' : t_value = S_COMMA; break;
			case '*' : t_value = S_STAR; break;
			case '=' : t_value = S_EQUAL; break;
			case '<' : t_value = S_LESS; break;
			case '>' : t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
		else if (*cur == '\'')
		{
			/* Find STRING_LITERRAL */
			int t_class;
			cur++;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

			temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else /* must be a ' */
			{
				add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
				cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}

	return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

	if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;

	tpd_list *t_list;
	t_list = (tpd_list*)calloc(1, sizeof(tpd_list));
	FILE *db_handle;
	db_handle=fopen("dbfile.bin","r");

	if(db_handle== NULL)
	{
		rc = FILE_OPEN_ERROR;
		 
	}
	else
	{
		//printf("HERE");
		fread(t_list,sizeof(tpd_list),1,db_handle);		
		fflush(db_handle);
		fclose(db_handle);
	}

	//printf("db flag::%d",t_list->db_flags);
	int db_flag = t_list->db_flags;

		bool unique = false;
		token_list *cur = tok_list;

		if ((cur->tok_value == K_CREATE) &&
			((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
		{
			if(db_flag != ROLLFORWARD_PENDING)
	        {
				printf("CREATE TABLE statement\n");
				cur_cmd = CREATE_TABLE;
				cur = cur->next->next;
			}
			else
			{
				printf("CANT ACCESS DB!! RF IN PROGRESS.. \n");

				
			}

		}
		else if ((cur->tok_value == K_DROP) &&
			((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
		{
				if(db_flag != ROLLFORWARD_PENDING)
				{

					printf("DROP TABLE statement\n");
					cur_cmd = DROP_TABLE;
					cur = cur->next->next;
				}
				else
				{
					printf("CANT ACCESS DB!! RF IN PROGRESS.. \n");
								
				}

		}
		else if ((cur->tok_value == K_LIST) &&
			((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
		{		
			if(db_flag != ROLLFORWARD_PENDING)
			{
				printf("LIST TABLE statement\n");
				cur_cmd = LIST_TABLE;
				cur = cur->next->next;
			}
			else
			{
				printf("CANT ACCESS DB!! RF IN PROGRESS.. \n");
								
			}
		}
		else if ((cur->tok_value == K_LIST) &&
			((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
		{
			
			if(db_flag != ROLLFORWARD_PENDING)
			{
				printf("LIST SCHEMA statement\n");
				cur_cmd = LIST_SCHEMA;
				cur = cur->next->next;
			}
			else
			{
				printf("CANT ACCESS DB!! RF IN PROGRESS.. \n");
								
			}
		}

		//nupur

		else if ( cur->tok_value == K_INSERT && cur->next != NULL && cur->next->tok_value==K_INTO )
		{	
			
			if(db_flag != ROLLFORWARD_PENDING)
			{
				printf("INSERT statement\n");
				cur_cmd = INSERT;
				cur = cur->next->next;
			}
			else
			{
				printf("CANT ACCESS DB!! RF IN PROGRESS.. \n");
								
			}
		}

		else if ( cur->tok_value == K_SELECT && cur->next != NULL)
		{	
			
			if(db_flag != ROLLFORWARD_PENDING)
			{
				printf("SELECT statement\n");
				cur_cmd = SELECT;
				cur = cur->next;
			}
			else
			{
				printf("CANT ACCESS DB!! RF IN PROGRESS.. \n");
								
			}
		}

		else if ( cur->tok_value == K_DELETE && cur->next != NULL && cur->next->tok_value==K_FROM)
		{	
			if(db_flag != ROLLFORWARD_PENDING)
			{			
				printf("DELETE statement\n");
				cur_cmd = DELETE;
				cur = cur->next->next;
			}
			else
			{
				printf("CANT ACCESS DB!! RF IN PROGRESS.. \n");
								
			}
		}

		else if( ( cur->tok_value == K_UPDATE) && cur->next != NULL  )
		{
			if(db_flag != ROLLFORWARD_PENDING)
			{						
				printf("UPDATE statement\n");
				cur_cmd = UPDATE;
				cur = cur->next;
			}
			else
			{
				printf("CANT ACCESS DB!! RF IN PROGRESS.. \n");
								
			}

		}

	
		else if( cur->tok_value == K_BACKUP && cur->next != NULL && cur->next->tok_value == K_TO)
		{
			if(db_flag != ROLLFORWARD_PENDING)
			{						
				printf("BACKUP statement\n");
				cur_cmd = BACKUP;
				cur = cur->next->next;	
			}
			else
			{
				printf("CANT ACCESS DB!! RF IN PROGRESS.. \n");
								
			}

		}

		else if( cur->tok_value == K_RESTORE && cur->next != NULL && cur->next->tok_value == K_FROM)
		{
			if(db_flag != ROLLFORWARD_PENDING)
			{						
				printf("RESTORE statement\n");
				cur_cmd = RESTORE;
				cur = cur->next->next;			
			}
			else
			{
				printf("CANT ACCESS DB!! RF IN PROGRESS.. \n");
								
			}
		}

		
		else if(cur->tok_value == K_ROLLFORWARD && cur->next != NULL)
		{
			printf("ROLLFORWARD statement\n");
			cur_cmd = ROLLFORWARD;
			cur = cur->next;	
		}

		//nupur

		else
		{
			printf("Invalid statement\n");
			rc = cur_cmd;
		}

		if (cur_cmd != INVALID_STATEMENT)
		{
			switch(cur_cmd)
			{
			case CREATE_TABLE:
				rc = sem_create_table(cur);
				log = 1;
				break;
			case DROP_TABLE:
				rc = sem_drop_table(cur);
				log = 1;
				break;
			case LIST_TABLE:
				rc = sem_list_tables();
				log = 0;
				break;
			case LIST_SCHEMA:
				rc = sem_list_schema(cur);
				log = 0;
				break;
				//nupur
			case INSERT:
				rc = sem_insert_table(cur);
				log = 1;
				break;

			case SELECT:
				rc= sem_select(cur);
				log = 0;
				break;

			case DELETE:
				rc = sem_delete_table(cur);
				log = 1;
				break;

			case UPDATE:
				rc = sem_update_table(cur);
				log = 1;
				break;
			case BACKUP:
				rc = sem_backup_table(cur);
				log = 1;
				break;

			case RESTORE:
				rc = sem_restore_from_table(cur);
				log = 0;
				break;
			case ROLLFORWARD:
				rc = sem_rollforward(cur);
				log = 0;
				break;
			default:
				break; /* no action */
			}
		}


	return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];


	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
						(cur->tok_class != identifier) &&
						(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
							/* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
								/* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;

								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										(cur->tok_value != K_NOT) &&
										(cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										col_entry[cur_id].col_len = sizeof(int);

										if ((cur->tok_value == K_NOT) &&
											(cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											(cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}

										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												(cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												if (cur->tok_value == S_RIGHT_PAREN)
												{
													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of S_INT processing
								else
								{
									// It must be char()
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;

										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;

											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;

												if ((cur->tok_value != S_COMMA) &&
													(cur->tok_value != K_NOT) &&
													(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														(cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
														(cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}

													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) &&															  (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
														{
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));

				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + 
						sizeof(cd_entry) *	tab_entry.num_columns;
					tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							(void*)&tab_entry,
							sizeof(tpd_entry));

						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
							(void*)col_entry,
							sizeof(cd_entry) * tab_entry.num_columns);

						rc = add_tpd_to_list(new_entry);

						free(new_entry);
					}
				}
			}
		}
	}

	//---create table file for each ----
	FILE *fp;

	char fileName[MAX_TOK_LEN];

	strcpy(fileName,tab_entry.table_name);
	strcat(fileName,".tab");


	fp = fopen(fileName,"wbc");

	if(fp ==NULL)
	{
		//printf("file error\n" );
		rc = FILE_OPEN_ERROR;
	}

	else	
	{	
		//printf("NO file error\n" );
		int size = 0;
		int a;
		int j;
		for(j=0; j <= cur_id; j++)
		{
			if(col_entry[j].col_type == T_CHAR)
			{		

				a = col_entry[j].col_len;
				size += (a*sizeof(char)) +1 ;

			}
			if(col_entry[j].col_type == T_INT)
			{ 

				size += sizeof(int) +1;
			}	
		}

		/*
		if(size%4 !=0)
		{	
		size = size -(size%4) +4;
		}

		*/

		//allocate memory to header
		table_file_header *file_header = NULL;
		file_header = (table_file_header*) malloc(sizeof(table_file_header));


		if(file_header == NULL)
		{		
			rc = MEMORY_ERROR;	

		}
		else
		{
			file_header->file_size = sizeof(table_file_header);
			file_header->record_size = size;
			file_header->num_records= 0;
			file_header->record_offset = sizeof(table_file_header);
			file_header->file_header_flag = 0;
			file_header->tpd_ptr = NULL;


			fwrite(file_header, sizeof(table_file_header) ,1,fp);

			printf("FILE CREATION SUCCESSFUL\n");
			printf("Header file size:%d\n",file_header->file_size);
			printf("Header Record Size:%d\n",file_header->record_size);
			printf("Header Num of Records:%d\n",file_header->num_records);
			printf("Header Record offset:%d\n",file_header->record_offset);
			printf("Header Flag:%d\n",file_header->file_header_flag);
			printf("Header pointer:%d\n",file_header->tpd_ptr);


		}

	}
	fflush(fp);
	fclose(fp);

	return rc;
}

int sem_insert_table(token_list *t_list)

{
	int flag = 0;

	int i;

	char temp[MAX_TOK_LEN];
	int j;
	int rc = 0;
	token_list *cur ;
	cur = t_list;
	char* tab[20];
	tpd_entry *tab_entry;
	cd_entry *col_entry;
	record_new_entry *record_list = NULL;
	record_list = (record_new_entry*)calloc(1,sizeof(record_new_entry) );

	*tab = cur->tok_string;

	tab_entry = get_tpd_from_list(*tab);
	strcat(*tab,".tab");
	//printf("%s\n",*tab);

	if(tab_entry == NULL)
	{
		printf("Invalid Table Name\n" );		
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
	}

	else 
	{
		//printf(" entering else\n");
		cur=cur->next;
		if(cur->tok_value == K_VALUES && cur->next->tok_value == S_LEFT_PAREN )

		{
			//printf("entering if \n");
			cur = cur->next->next;

			//match the column types with token list one by one
			int numCol = tab_entry->num_columns;
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);


			for( i=1; i<=numCol;i++)
			{
				//printf("Entering for \n");			
				//printf( "%s\n" ,cur->tok_string);


				if (( col_entry->col_type  == T_CHAR ) && (cur->tok_value== STRING_LITERAL) )
				{
					//printf("nupur...\n");	

					if(col_entry->col_len >= strlen(cur->tok_string))
					{
						// validation is done.
						//	printf(" Entered correct string\n");
						//store it in list
						strcpy(temp ,cur->tok_string);
						//printf("%s\n", temp);
						add_to_recordList( &record_list, temp ,col_entry->col_len, col_entry->col_type);


					}
					else
					{
						printf("Maximum Length Exceeded");
						rc = MAX_LENGTH_EXCEEDED;
						break;

					}

				}

				else if ( (col_entry->col_type == T_INT) && (cur->tok_value==INT_LITERAL) )
				{
					//printf("Entered correct Integer\n");
					add_to_recordList( &record_list, cur->tok_string,col_entry->col_len,col_entry->col_type);


				}	

				else if(cur->tok_value == K_NULL)
				{

					// check if null is allowed
					if(col_entry->not_null == 1)
					{
						printf("Null not allowed for %s\n",col_entry->col_name);
						rc = INVALID_NOT_NULL_CONSTRAINT;
						cur->tok_value = INVALID;
						break;

					}
					else
						//printf("passed null constraint\n");					    
						add_to_recordList( &record_list, cur->tok_string, col_entry->col_len,col_entry->col_type);


				}
				//for checking if column is less than specified

				else if(cur->tok_value == S_RIGHT_PAREN)
				{

					printf("Entered less number of columns than required\n");
					rc = COMPLETE_COL_NOT_DEFINED;
					cur->tok_value = INVALID;
					break;

				}
				else
				{
					printf("Types do not match for %d column\n", i);
					rc = DATATYPE_MISMATCH_ERROR ;
					cur->tok_value = INVALID;
					break; //come out of loop as col types does not match
				}

				//   printf("Going to next token\n");

				cur=cur->next;


				if( (cur->tok_value != S_RIGHT_PAREN) && i < numCol ) //defines when to expect comma
				{
					if(cur->tok_value == S_COMMA)
					{
						//printf("move to the next column ");
						cur = cur->next;
						col_entry++;

					}

					else
					{
						rc =INVALID_INSERT_DEFINITION;
						cur->tok_value = INVALID;
						printf("Comma is not present \n");
						break; //come out of loop as comma is not present
					}

				}


			}//for ends

			if(i > numCol )

			{

				if ((cur->tok_value== S_RIGHT_PAREN) && (cur->next->tok_value ==EOC) )
				{
					//flag = 1;
					printf( "Insert Parsing successful!!\n");
					rc = add_record_to_file(record_list, *tab);

				}
				else if( (cur->tok_value== S_RIGHT_PAREN) && (cur->next->tok_value != EOC) )
				{
					printf("Trying to exceed the no. of column allowed\n ");
					rc =INVALID_INSERT_DEFINITION;
					cur->tok_value = INVALID;
				}

				else if(cur->tok_value != S_RIGHT_PAREN)
				{			
					printf("Trying to exceed the no. of column allowed\n ");
					rc =INVALID_INSERT_DEFINITION;
					cur->tok_value = INVALID;

				}

			}

		} // if for "value and left parenthesis"
		else
		{
			rc = INVALID_INSERT_DEFINITION;
			cur->tok_value = INVALID;
			printf("Insert definition is not correct\n");
		} // else for "value and parenthesis"


	}// else for most outer if checking table definition

	return rc;
} 

void add_to_recordList(record_new_entry **record_list, char *value, int col_len1, int type)

{
	//printf("entering add_to_recordList\n");

	record_new_entry *cur = *record_list;
	record_new_entry *ptr = NULL;
	ptr= (record_new_entry*)calloc(1,sizeof(record_new_entry));

	//printf("copying value\n");
	if(type == T_CHAR)
	{
		strcpy(ptr->record_string ,value);
		ptr->col_len = strlen(value);
		ptr->max_length = col_len1;
		ptr->cType = T_CHAR;
		ptr->next = NULL;	
	}

	else if(type== T_INT)

	{
		strcpy(ptr->record_string ,value);
		ptr->col_len = 4;
		ptr->max_length = 4;
		ptr->cType = T_INT;
		ptr->next = NULL;	


	}

	/*	strcpy(ptr->record_string ,value);
	ptr->col_len = col_len1;
	printf("copying value\n");

	ptr->next = NULL;
	*/
	if( cur == NULL)
	{
		//printf("enter if\n");
		*record_list = ptr;

	}
	else
	{
		//  printf("entering else \n");
		while(cur ->next != NULL)
			cur=cur->next;	

		cur->next=ptr;

	}

	return;
}

int add_record_to_file(record_new_entry *recList, char *fileName)

{

	int rc =0 ,old_size;
	FILE *fhandle = NULL;

	struct _stat file_stat;
	table_file_header *header;

	if ((fhandle = fopen( fileName,"rbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;

	}	
	else
	{
		_fstat(_fileno(fhandle), &file_stat);
		printf("OLD File size:%d\n",file_stat.st_size);

		header= (table_file_header*)calloc(1 ,file_stat.st_size);

		if(!header)
		{
			rc = MEMORY_ERROR;

		}

		else
		{

			fread(header, file_stat.st_size, 1, fhandle);

			fflush(fhandle);
			fclose(fhandle);

			if (header->file_size != file_stat.st_size)

			{

				rc = DBFILE_CORRUPTION;

			}

		}

	}

	/*now update the header structure
	create a new file and new data to it everytime u add a record */

	if((fhandle = fopen(fileName, "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}

	else
	{
		// printf("writing the record into the file\n");
		old_size = header->file_size;
		header->num_records += 1; 
		header->file_size += header->record_size; 
		//writing the header structure
		fwrite(header, old_size , 1, fhandle);

		recList = recList->next;

		while(recList->next != NULL)
		{
			//fwrite(ptr,1,1,fp);
			if(recList->cType == T_INT)
			{

				if( strcmpi( recList->record_string ,"NULL")== 0)
				{
					//printf("check\n");
					char m = 0;
					fwrite(&m,1,1,fhandle);						
					int a = atoi(recList->record_string);
					fwrite(&a ,4,1,fhandle);


				}

				else
				{
					//printf("check2\n");
					int a = atoi(recList->record_string);
					fwrite(recList,1,1,fhandle);
					fwrite(&a ,4,1,fhandle);

				}
			}
			else
			{
				if( strcmpi(recList->record_string, "NULL")==0)
				{

					//printf("check3\n");
					recList->col_len = 0;
					fwrite(recList,recList->max_length +1,1,fhandle);

				}

				else
				{

					//printf("check4\n");
					fwrite(recList,recList->max_length +1,1,fhandle);

				}
			}			
			recList=recList->next;

		}

		//for last element
		if(recList->cType == T_INT)
		{
			if(strcmpi( recList->record_string ,"NULL")==0)
			{

				//printf("check\n");
				char m = 0;
				fwrite(&m,1,1,fhandle);						
				int a = atoi(recList->record_string);
				fwrite(&a ,4,1,fhandle);

			}

			else
			{
				//printf("check2");
				int a = atoi(recList->record_string);
				fwrite(recList,1,1,fhandle);
				fwrite(&a ,4,1,fhandle);
			}				
		}


		else
		{
			if((strcmpi( recList->record_string ,"NULL")==0))
			{

				//printf("check3\n");
				recList->col_len = 0;
				fwrite(recList,recList->max_length +1,1,fhandle);
			}

			else
			{
				//printf("check4\n");
				fwrite(recList,recList->max_length+1,1,fhandle);
			}
		}

		fflush(fhandle);
		fclose(fhandle);

	}

	printf("NEW file size:%d\n", header->file_size);

	return rc;
}


int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
			}
		}
	}

	return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

	return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			(cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;

					if ((cur->tok_class != keyword) &&
						(cur->tok_class != identifier) &&
						(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
						printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
							fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
							i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}

						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

	return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
	struct _stat file_stat;

	/* Open for read */
	if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));

			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
		_fstat(_fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}

	return rc;
}

int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
			g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
			g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
				else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								old_size - cur->tpd_size -
								(sizeof(tpd_list) - sizeof(tpd_entry)),
								1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
						g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
							1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						has already subtracted the cur->tpd_size, therefore it will
						point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
								old_size - cur->tpd_size -
								((char*)cur - (char*)g_tpd_list),							     
								1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}


			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}

	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}


// select statement
//select * from Tablename

int sem_select(token_list *t_list)
{
	int desc ;
	int flag = 0;
	int rc = 0;
	token_list * cur;
	token_list *cur2;
	cur = t_list;
	cur2 = t_list;
	tpd_entry *tab_entry;
	char *tab;
	char col_order_name[MAX_IDENT_LEN];
	int col_order_type;

	where_list *where_col_list = NULL;
	where_col_list = (where_list*)calloc(MAX_NUM_COL,sizeof(where_list) );


	// check for *
	if (cur->tok_value == S_STAR)
	{

		printf("SELECT WITH STAR\n");
		cur = cur->next;

		if (cur->tok_value != K_FROM)
		{
			printf("From not present\n");
			rc = INVALID_SELECT_STATEMENT;
			cur->tok_class = INVALID;

		}

		else
		{
			cur = cur->next;
			/*		  if ((cur->tok_class != keyword) &&
			(cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
			{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;

			}
			*/		
			//		  else
			//		  {
			tab = cur->tok_string;
			tab_entry = get_tpd_from_list(tab);


			if( tab_entry == NULL)
			{

				printf("table name not valid\n");
				rc= TABLE_NOT_EXIST;
				cur->tok_class = INVALID;

			}

			else
			{
				cur = cur->next;

				if(cur->tok_value == EOC)
				{
					printf("Select Parsing successful\n");
					// call the function to display the data
					rc = select_from_file(tab);

					if(rc != 0)
					{
						printf("Couldn't select from file\n");
					}
					else
					{
						printf("SELECT * successful\n");
					
					}

				}

				else
				{
					if(cur->tok_value == K_WHERE)
					{
						//printf("Found WHERE\n");
						cur= cur->next;

						if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
						{
							// Error
							printf("Expecting a vaid column string after WHERE\n");
							rc = INVALID_WHERE_CLAUSE;
							cur->tok_value = INVALID;

						}

						else
						{


							for( int i=1 ; i <= MAX_NUM_COL ; i++)
							{
								//printf("Start of for loop\n");

								int flag = -1;

								//printf("token class:%d\n",cur->tok_class);
								if ((cur->tok_class != keyword) &&
									(cur->tok_class != identifier) &&
									(cur->tok_class != type_name))
								{
									printf("Expecting a valid column string after WHERE\n");
									rc = INVALID_WHERE_CLAUSE;
									cur->tok_value = INVALID;
									break;

								}

								else
								{

									int  y = get_col_from_table(cur->tok_string,tab_entry->table_name);

									if (y != 0)
									{

										printf("column not found in table file\n");
										rc = COL_NOT_EXIST;
										cur->tok_value = INVALID;
										break;
									}

									else
									{
										int col_type;
										col_type = get_colType_from_table(cur->tok_string,tab_entry->table_name);

										//printf("Found column\n");								
										if( (cur->next->tok_value == S_EQUAL) || (cur->next->tok_value == S_LESS) || (cur->next->tok_value == S_GREATER) )
										{
											//printf("Some Symbol present\n");

											if(cur->next->next->tok_value == STRING_LITERAL)
											{
												if (col_type == T_CHAR)
												{
													if( (cur->next->tok_value == S_EQUAL) || (cur->next->tok_value == S_LESS) || (cur->next->tok_value == S_GREATER) )
													{
														//printf("String present\n");
														if( (cur->next->next->next->tok_value == K_AND) || (cur->next->next->next->tok_value == K_OR) )
														{
															//printf("AND/OR present after string\n");

															add_to_where_list(&where_col_list ,cur->tok_string, cur->next->tok_value,cur->next->next->tok_string ,flag, cur->next->next->next->tok_value,T_CHAR);																				
															cur = cur->next->next->next;
															printf("%s:",cur->tok_string);

														}

														else
														{

															if(cur->next->next->next->tok_value == EOC)
															{
																//printf("EOC present after String\n");
																//printf("GUN\n");
																add_to_where_list(&where_col_list ,cur->tok_string, cur->next->tok_value,cur->next->next->tok_string , flag, EOC,T_CHAR);
																rc = select_all_where_clause(where_col_list,tab);
																//printf("rc:%d",rc);
																//printf("XggX\n");
																break;
															}

															else if (cur->next->next->next->tok_value == K_ORDER)
															{
																//printf("XXX%s",cur->tok_string);
																add_to_where_list(&where_col_list ,cur->tok_string, cur->next->tok_value,cur->next->next->tok_string , flag, EOC,T_CHAR);
																//order by for select * without where clause 
																//printf("entering ORDER BY NUPUR\n");

																cur = cur->next->next->next->next;

																//printf("%s:",cur->tok_string);
																if (cur->tok_value == K_BY)
																{
																	cur = cur ->next;
																	if (cur->tok_class == identifier)
																	{
																		strcpy(col_order_name ,cur->tok_string); 
																		if( get_col_from_table(cur->tok_string, tab) == 0)
																		{
																			col_order_type = get_colType_from_table(cur->tok_string, tab);
																			cur =cur->next;

																			if(cur->tok_value==K_DESC)
																			{
																				desc =1;
																				//printf("Found Descending\n");
																				cur= cur->next;

																				if(cur->tok_value == EOC)
																				{
																					//call order by
																					printf("Parsing SUCC\n");
																					//add_to_where_list(&where_col_list ,cur->tok_string, cur->next->tok_value,cur->next->next->tok_string , flag, EOC,T_CHAR);
																					rc = select_order_by(where_col_list ,NULL, tab, col_order_name, col_order_type, desc);

																				}
																				else
																				{
																					rc= INVALID_SELECT_STATEMENT;
																					cur->tok_value = INVALID;
																					return rc;

																				}								  
																			}
																			else if (cur->tok_value == EOC )
																			{
																				desc = 0;
																				//printf("Found Ascending\n");
																				//add_to_where_list(&where_col_list ,cur->tok_string, cur->next->tok_value,cur->next->next->tok_string , flag, EOC,T_CHAR);
																				rc = select_order_by(where_col_list ,NULL, tab, col_order_name, col_order_type, desc);
																				printf("Parsing SUCC\n");

																			}

																			else
																			{
																				rc= INVALID_SELECT_STATEMENT;
																				cur->tok_value = INVALID;
																				return rc;

																			}

																		}
																		else
																		{
																			rc= COL_NOT_EXIST;
																			cur->tok_value = INVALID;
																			return rc;

																		}

																	}

																	else
																	{
																		rc= INVALID_COLUMN_NAME;
																		cur->tok_value = INVALID;
																		return rc;

																	}

																}
																else
																{
																	rc = INVALID_ORDER_BY;
																	cur->tok_value = INVALID;
																	return rc;

																}

																break;
															} //order by

															else
															{

																printf("eoc or AND/OR missing, not ending properly\n");
																//error
																rc = INVALID_WHERE_CLAUSE;
																cur->next->next->next->tok_value = INVALID;
																break;
															}
														}
													} //symbol check for string

													else
													{
														printf("Symbol for string is not accepted\n");
														rc = INVALID_SYMBOL;
														cur->next->tok_value = INVALID;
														break;
													}
												}

												else
												{
													printf("DATA MISMATCH\n");
													rc = DATATYPE_MISMATCH_ERROR;
													cur->next->next->tok_value = INVALID;
													return rc;

												}

											}//string literal

											else if(cur->next->next->tok_value == INT_LITERAL)
											{
												if(col_type==T_INT)
												{
													//printf("Numeric present\n");
													if( (cur->next->next->next->tok_value == K_AND) || (cur->next->next->next->tok_value == K_OR) )
													{
														//printf("AND/OR present after numeric\n");

														add_to_where_list(&where_col_list ,cur->tok_string, cur->next->tok_value,cur->next->next->tok_string ,flag, cur->next->next->next->tok_value,T_INT);							

														cur = cur->next->next->next;
														//printf("%s::",cur->tok_string);
													}

													else
													{

														if(cur->next->next->next->tok_value == EOC)
														{
															//printf("EOC present after numeric\n");

															add_to_where_list(&where_col_list ,cur->tok_string, cur->next->tok_value,cur->next->next->tok_string , flag, EOC,T_INT);
															rc = select_all_where_clause(where_col_list,tab);
															//printf("NUP\n");
															//rc = select_order_by(where_col_list ,NULL, tab, "age", T_INT,1);
															//rc = select_order_by(NULL ,NULL, tab, "gender", T_CHAR, 1);
															break;
														}

														else if (cur->next->next->next->tok_value == K_ORDER)
														{
															add_to_where_list(&where_col_list ,cur->tok_string, cur->next->tok_value,cur->next->next->tok_string , flag, EOC,T_INT);
															//order by for select * without where clause 
															//printf("entering ORDER BY NUPUR\n");

															cur = cur->next->next->next->next;

															//printf("%s:",cur->tok_string);
															if (cur->tok_value == K_BY)
															{
																cur = cur ->next;
																if (cur->tok_class == identifier)
																{
																	strcpy(col_order_name ,cur->tok_string);
																	if( get_col_from_table(cur->tok_string,tab) == 0)
																	{
																		col_order_type = get_colType_from_table(cur->tok_string,tab);
																		cur =cur->next;

																		if(cur->tok_value==K_DESC)
																		{
																			desc =1;
																			//printf("Found Descending\n");
																			cur= cur->next;

																			if(cur->tok_value == EOC)
																			{
																				rc = select_order_by(where_col_list ,NULL, tab, col_order_name, col_order_type, desc);
																				printf("Parsing SUCC\n");

																			}
																			else
																			{
																				rc= INVALID_SELECT_STATEMENT;
																				cur->tok_value = INVALID;
																				return rc;

																			}								  
																		}
																		else if (cur->tok_value == EOC )
																		{
																			desc = 0;
																			//printf("Found Ascending\n");
																			//call order by
																			rc = select_order_by(where_col_list ,NULL, tab, col_order_name, col_order_type, desc);
																			printf("Parsing SUCC\n");

																		}

																		else
																		{
																			rc= INVALID_SELECT_STATEMENT;
																			cur->tok_value = INVALID;
																			return rc;

																		}

																	}
																	else
																	{
																		rc= COL_NOT_EXIST;
																		cur->tok_value = INVALID;
																		return rc;

																	}

																}

																else
																{
																	rc= INVALID_COLUMN_NAME;
																	cur->tok_value = INVALID;
																	return rc;

																}

															}
															else
															{
																rc = INVALID_ORDER_BY;
																cur->tok_value = INVALID;
																return rc;

															}

															break;
														} //order by

														else
														{

															printf("eoc or AND/OR missing, not ending properly\n");
															//error
															rc = INVALID_WHERE_CLAUSE;
															cur->next->next->next->tok_value = INVALID;
															break;
														}
													}
												} //t-int
												else
												{
													printf("DATA MISMATCH\n");
													rc = DATATYPE_MISMATCH_ERROR;
													cur->next->next->tok_value = INVALID;
													return rc;

												}
											}//int-literal

											else
											{

												printf("constant not correct/present\n");
												rc = INVALID_WHERE_CLAUSE;
												cur->next->next->tok_value = INVALID;
												break;
												//error
											}


										}

										else if( (cur->next->tok_value == K_IS) && (cur->next->next->tok_value == K_NULL) )
										{
											flag = 1;
											//printf("Found is null condition\n");

											if( (cur->next->next->next->tok_value == K_AND) || (cur->next->next->next->tok_value == K_OR) )
											{

												//printf("AND OR present\n");
												add_to_where_list(&where_col_list ,cur->tok_string,0,"", flag ,cur->next->next->next->tok_value ,0);							

												cur = cur->next->next->next;
											}

											else
											{

												if(cur->next->next->next->tok_value == EOC)
												{
													//printf("EOC present\n");

													add_to_where_list(&where_col_list ,cur->tok_string, 0 ,"",flag , EOC ,0);
													rc = select_all_where_clause(where_col_list,tab);
													//printf("NN11\n");
													break;
												}

												else if (cur->next->next->next->tok_value == K_ORDER)
												{
													add_to_where_list(&where_col_list ,cur->tok_string, 0 ,"",flag , EOC ,0);
													//order by for select * without where clause 
													//printf("entering ORDER BY NUPUR\n");

													cur = cur->next->next->next->next;

													//printf("%s:",cur->tok_string);
													if (cur->tok_value == K_BY)
													{
														cur = cur ->next;
														if (cur->tok_class == identifier)
														{
															strcpy(col_order_name ,cur->tok_string);
															if( get_col_from_table(cur->tok_string,tab) == 0)
															{
																col_order_type = get_colType_from_table(cur->tok_string, tab);
																cur =cur->next;

																if(cur->tok_value==K_DESC)
																{
																	desc =1;
																	//printf("Found Descending\n");
																	cur= cur->next;

																	if(cur->tok_value == EOC)
																	{
																		//call order by
																		printf("Parsing SUCC\n");
																		rc = select_order_by(where_col_list ,NULL, tab, col_order_name, col_order_type, desc);

																	}
																	else
																	{
																		rc= INVALID_SELECT_STATEMENT;
																		cur->tok_value = INVALID;
																		return rc;

																	}								  
																}
																else if (cur->tok_value == EOC )
																{
																	desc = 0;
																	//printf("Found Ascending\n");
																	//call order by
																	printf("Parsing SUCC\n");
																	rc = select_order_by(where_col_list ,NULL, tab, col_order_name, col_order_type, desc);

																}

																else
																{
																	rc= INVALID_SELECT_STATEMENT;
																	cur->tok_value = INVALID;
																	return rc;

																}

															}
															else
															{
																rc= COL_NOT_EXIST;
																cur->tok_value = INVALID;
																return rc;

															}

														}

														else
														{
															rc= INVALID_COLUMN_NAME;
															cur->tok_value = INVALID;
															return rc;

														}

													}
													else
													{
														rc = INVALID_ORDER_BY;
														cur->tok_value = INVALID;
														return rc;

													}

													break;
												} //order by		

												else
												{

													printf("eoc or AND/OR missing, not ending properly\n");
													//error
													rc = INVALID_WHERE_CLAUSE;
													cur->next->next->next->tok_value = INVALID;
													break;
												}
											}
										} // is null

										else if( (cur->next->tok_value == K_IS) && (cur->next->next->tok_value == K_NOT) && (cur->next->next->next->tok_value == K_NULL) )
										{
											flag = 0;
											//printf("Found is NOT null condition\n");

											if( (cur->next->next->next->next->tok_value == K_AND) || (cur->next->next->next->next->tok_value == K_OR) )
											{
												//printf("AND OR present\n");
												add_to_where_list(&where_col_list ,cur->tok_string, 0," ",flag ,cur->next->next->next->next->tok_value, 0);

												cur = cur->next->next->next->next;
											}

											else
											{

												if(cur->next->next->next->next->tok_value == EOC)
												{
													//printf("EOC present\n");
													add_to_where_list(&where_col_list ,cur->tok_string,0,"",flag,EOC,0  );
													rc = select_all_where_clause(where_col_list,tab);

													break;
												}

												else if (cur->next->next->next->next->tok_value == K_ORDER)
												{
													add_to_where_list(&where_col_list ,cur->tok_string,0,"",flag,EOC,0  );
													//order by for select * without where clause 
													//printf("entering ORDER BY NUPUR\n");

													cur = cur->next->next->next->next->next;

													printf("%s:",cur->tok_string);
													if (cur->tok_value == K_BY)
													{
														cur = cur ->next;
														if (cur->tok_class == identifier)
														{
															strcpy(col_order_name ,cur->tok_string); 
															if( get_col_from_table(cur->tok_string,tab) == 0)
															{
																col_order_type = get_colType_from_table(cur->tok_string, tab);
																cur =cur->next;

																if(cur->tok_value==K_DESC)
																{
																	desc =1;
																	//printf("Found Descending\n");
																	cur= cur->next;

																	if(cur->tok_value == EOC)
																	{
																		//call order by
																		printf("Parsing SUCC\n");
																		rc = select_order_by(where_col_list ,NULL, tab, col_order_name, col_order_type, desc);

																	}
																	else
																	{
																		rc= INVALID_SELECT_STATEMENT;
																		cur->tok_value = INVALID;
																		return rc;

																	}								  
																}
																else if (cur->tok_value == EOC )
																{
																	desc = 0;
																	//printf("Found Ascending\n");
																	//call order by
																	printf("Parsing SUCC\n");
																	rc = select_order_by(where_col_list ,NULL, tab, col_order_name, col_order_type, desc);

																}

																else
																{
																	rc= INVALID_SELECT_STATEMENT;
																	cur->tok_value = INVALID;
																	return rc;

																}

															}
															else
															{
																rc= COL_NOT_EXIST;
																cur->tok_value = INVALID;
																return rc;

															}

														}

														else
														{
															rc= INVALID_COLUMN_NAME;
															cur->tok_value = INVALID;
															return rc;

														}

													}
													else
													{
														rc = INVALID_ORDER_BY;
														cur->tok_value = INVALID;
														return rc;

													}

													break;
												} //order by

												else
												{

													printf("eoc or AND/OR missing, not ending properly\n");
													//error
													rc = INVALID_WHERE_CLAUSE;
													cur->next->next->next->next->tok_value = INVALID;
													break;
												}
											}

										}

										else
										{

											printf("Not a valid operation defined after column name\n");
											rc = INVALID_WHERE_CLAUSE;
											cur->tok_value = INVALID;
											break;
										}							  


									}//else

								} //else

								//printf("T.C %d:",cur->tok_class);
								cur = cur->next;

								//printf("Just before loop:%s\n",cur->tok_string);
								//printf("T.C %d:",cur->tok_class);
								//printf("Just before loop");
							} //for


						}//else for valid col name

					} //if for where clause

					else if( cur->tok_value== K_ORDER)
					{
						//order by for select * without where clause 
						//printf("entering ORDER BY withot where clause\n");


						cur = cur->next;
						if (cur->tok_value == K_BY)
						{
							cur = cur ->next;
							if (cur->tok_class == identifier)
							{
								strcpy(col_order_name ,cur->tok_string); 
							
								if( get_col_from_table(cur->tok_string,tab) == 0)
								{
									col_order_type = get_colType_from_table(cur->tok_string, tab);
									cur =cur->next;

									if(cur->tok_value==K_DESC)
									{
										desc =1;
										//printf("Found Descending\n");
										cur= cur->next;

										if(cur->tok_value == EOC)
										{
											//call order by
											printf("Parsing SUCC\n");
											rc = select_order_by(NULL ,NULL, tab, col_order_name, col_order_type, desc);


										}
										else
										{
											rc= INVALID_SELECT_STATEMENT;
											cur->tok_value = INVALID;
											return rc;

										}								  
									}
									else if (cur->tok_value == EOC )
									{
										desc = 0;
										//printf("Found Ascending\n");
										//call order by
										printf("Parsing SUCC\n");
										rc = select_order_by(NULL ,NULL, tab, col_order_name, col_order_type, desc);

									}

									else
									{
										rc= INVALID_SELECT_STATEMENT;
										cur->tok_value = INVALID;
										return rc;

									}

								}
								else
								{
									rc= COL_NOT_EXIST;
									cur->tok_value = INVALID;
									return rc;

								}


							}

							else
							{
								rc= INVALID_COLUMN_NAME;
								cur->tok_value = INVALID;
								return rc;

							}

						}
						else
						{
							rc = INVALID_ORDER_BY;
							cur->tok_value = INVALID;
							return rc;

						}
					} //order by

					else
					{

						printf(" Select statement not valid, expecting a where or ORDER BY clause \n");
						rc = INVALID_WHERE_CLAUSE;
						cur->tok_value = INVALID;

					}
				} //where
			}

			//		  }

		}

		//printf("Freeing the pointer\n");
		free(where_col_list);
		//printf("DONE\n");
	}

	// check for col types
	else if( cur->tok_class == identifier)
	{
		printf("ENTERING SELECT WITH MUTIPLE COLUMNS\n");
		column_list *col_list = NULL;
		col_list = (column_list*)calloc(1,sizeof(column_list) );

		int i;
		int j;
		char *col_val = NULL;
		col_val= (char*)calloc(MAX_IDENT_LEN,sizeof(char));

		for(  i =1 ; i<=MAX_NUM_COL; i++)
		{
			if(cur->tok_class == identifier)
			{
				//printf("%s..", cur->tok_string);
				strcpy(col_val,cur->tok_string);
				//printf("%s..",col_val);

				cur = cur->next;
				if(cur->tok_value == S_COMMA)
				{
					// printf("found comma\n");
				}

				else if( cur->tok_value == K_FROM && cur->tok_class==keyword)
				{
					//go and check the table
					//printf("found from\n");
					break;

				}
				else
				{

					printf(" comma or from absent\n ");
					rc = INVALID_SELECT_STATEMENT;
					break;
				}

				cur =cur->next;
			} //if 

			else 
			{
				printf("didnt find identifier after iden %d\n", i-1);
				break;
			}

		} //for 

		if (cur->tok_value==K_FROM )
		{
			cur = cur->next;
			if( cur->tok_class == identifier)
			{
				printf("Found table name..%s\n",cur->tok_string);
				printf("check if table exists in dbfile\n");

				tab_entry = get_tpd_from_list(cur->tok_string);
				char * tab = tab_entry->table_name;

				if(tab_entry == NULL)
				{
					printf("table not exists\n");
					rc = TABLE_NOT_EXIST;

				}

				else
				{
					printf("table exists in dbfile\n");
					printf("now check if all column passes\n");

					cur = cur->next;

					if (cur->tok_value == EOC)
					{
						//	printf("value of i %d\n",i);
						for( j =1; j <=i; j++ )
						{
							//	printf("%s\n",cur2->tok_string);
							int  x = get_col_from_table(cur2->tok_string,tab_entry->table_name);


							if (x == 0 )
							{
								printf("Found col%d\n", j);
								add_to_colList(&col_list,cur2->tok_string);

							}
							else
							{
								printf("DIDNT FIND col-%d\n",j);
								rc = COL_NOT_EXIST;
								//break;
								cur2->tok_value= INVALID;
								return rc;
							}					
							cur2 = cur2->next->next;

						}//for2 ends here 

						if (j-1 == i)
						{

							printf("you found all the columns , lets select the values from table\n");

							//call function to display
							int x = select_col_from_file(col_list, tab);

						}

					} // for EOC

					else if(cur->tok_value == K_WHERE)
					{
						for( j =1; j <=i; j++ )
						{
							//	printf("%s\n",cur2->tok_string);
							int  x = get_col_from_table(cur2->tok_string,tab_entry->table_name);


							if (x == 0 )
							{
								printf("Found col%d\n", j);
								add_to_colList(&col_list,cur2->tok_string);

							}
							else
							{
								printf("didnt Find col NUPUR%d\n",j);
								rc = COL_NOT_EXIST;
								cur2->tok_value= INVALID;
								return rc;
								//break;
							}					
							cur2 = cur2->next->next;

						}//for2 ends here 

						if (j-1 == i)
						{

							printf("you found all the columns , lets select the values from table\n");

							//call function to display
							//int x = select_col_from_file(col_list, tab);

						}

						//--------------------------------------------------------------------------------------------------
						printf("Found WHERE0\n");
						cur= cur->next;

						if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
						{
							// Error
							printf("Expecting a vaid column string after WHERE\n");
							rc = INVALID_WHERE_CLAUSE;
							cur->tok_value = INVALID;

						}

						else
						{

							for( int i=1 ; i <= MAX_NUM_COL ; i++)
							{

								int flag = -1;
								if ((cur->tok_class != keyword) &&
									(cur->tok_class != identifier) &&
									(cur->tok_class != type_name))
								{
									printf("Expecting a valid column string after WHERE\n");
									rc = INVALID_WHERE_CLAUSE;
									cur->tok_value = INVALID;
									break;

								}

								else
								{

									int  y = get_col_from_table(cur->tok_string,tab_entry->table_name);

									if (y != 0)
									{

										printf("column not found in table file\n");
										rc = COL_NOT_EXIST;
										cur->tok_value = INVALID;
										break;
									}

									else
									{

										int col_type;
										col_type = get_colType_from_table(cur->tok_string,tab_entry->table_name);

										printf("Found column\n");								
										if( (cur->next->tok_value == S_EQUAL) || (cur->next->tok_value == S_LESS) || (cur->next->tok_value == S_GREATER) )
										{
											printf("Some Symbol present\n");

											if(cur->next->next->tok_value == STRING_LITERAL)
											{
												if (col_type == T_CHAR)
												{
													
													if( (cur->next->tok_value == S_EQUAL) || (cur->next->tok_value == S_LESS) || (cur->next->tok_value == S_GREATER) )
													{												
														printf("String present\n");
														if( (cur->next->next->next->tok_value == K_AND) || (cur->next->next->next->tok_value == K_OR) )
														{
															printf("AND/OR present after string\n");

															add_to_where_list(&where_col_list ,cur->tok_string, cur->next->tok_value,cur->next->next->tok_string ,flag, cur->next->next->next->tok_value,T_CHAR);																				
															cur = cur->next->next->next;
														}

														else
														{

															if(cur->next->next->next->tok_value == EOC)
															{
																printf("EOC present after String\n");

																add_to_where_list(&where_col_list ,cur->tok_string, cur->next->tok_value,cur->next->next->tok_string , flag, EOC,T_CHAR);
																rc = select_col_where_clause(where_col_list,tab,col_list);
																break;
															}

															else if (cur->next->next->next->tok_value == K_ORDER)
															{
																add_to_where_list(&where_col_list ,cur->tok_string, cur->next->tok_value,cur->next->next->tok_string , flag, EOC,T_CHAR);
																//order by for select * without where clause 
																printf("entering ORDER BY NUPUR\n");

																cur = cur->next->next->next->next;

																printf("%s:",cur->tok_string);
																if (cur->tok_value == K_BY)
																{
																	cur = cur ->next;
																	if (cur->tok_class == identifier)
																	{
																		strcpy(col_order_name ,cur->tok_string); 
							
																		if( get_col_from_table(cur->tok_string,tab) == 0)
																		{
																			col_order_type = get_colType_from_table(cur->tok_string, tab);
																			cur =cur->next;

																			if(cur->tok_value==K_DESC)
																			{
																				desc =1;
																				printf("Found Descending\n");
																				cur= cur->next;

																				if(cur->tok_value == EOC)
																				{
																					//call order by
																					printf("Parsing SUCC\n");
																					rc = select_order_by(where_col_list ,col_list, tab, col_order_name, col_order_type, desc);



																				}
																				else
																				{
																					rc= INVALID_SELECT_STATEMENT;
																					cur->tok_value = INVALID;
																					return rc;

																				}								  
																			}
																			else if (cur->tok_value == EOC )
																			{
																				desc = 0;
																				printf("Found Ascending\n");
																				//call order by
																				printf("Parsing SUCC\n");
																				rc = select_order_by(where_col_list ,col_list, tab, col_order_name, col_order_type, desc);

																			}

																			else
																			{
																				rc= INVALID_SELECT_STATEMENT;
																				cur->tok_value = INVALID;
																				return rc;

																			}

																		}
																		else
																		{
																			rc= COL_NOT_EXIST;
																			cur->tok_value = INVALID;
																			return rc;

																		}

																	}

																	else
																	{
																		rc= INVALID_COLUMN_NAME;
																		cur->tok_value = INVALID;
																		return rc;

																	}

																}
																else
																{
																	rc = INVALID_ORDER_BY;
																	cur->tok_value = INVALID;
																	return rc;

																}

																break;
															} //order by



															else
															{

																printf("eoc or AND/OR missing, not ending properly\n");
																//error
																rc = INVALID_WHERE_CLAUSE;
																cur->next->next->next->tok_value = INVALID;
																break;
															}
														}
													}//symbol check for string
													else
													{
														printf("Symbol for string is not accepted\n");
														rc = INVALID_SYMBOL;
														cur->next->tok_value = INVALID;
														break;

													}
												} // t_char
												else
												{
													printf("DATA MISMATCH\n");
													rc = DATATYPE_MISMATCH_ERROR;
													cur->next->next->tok_value = INVALID;
													return rc;

												}
											}//String-literal

											else if(cur->next->next->tok_value == INT_LITERAL)
											{
												if(col_type == T_INT)
												{
													printf("Numeric present\n");
													if( (cur->next->next->next->tok_value == K_AND) || (cur->next->next->next->tok_value == K_OR) )
													{
														printf("AND/OR present after numeric\n");

														add_to_where_list(&where_col_list ,cur->tok_string, cur->next->tok_value,cur->next->next->tok_string ,flag, cur->next->next->next->tok_value,T_INT);							

														cur = cur->next->next->next;
														//printf("%s::",cur->tok_string);
													}

													else
													{

														if(cur->next->next->next->tok_value == EOC)
														{
															printf("EOC present after numeric\n");

															add_to_where_list(&where_col_list ,cur->tok_string, cur->next->tok_value,cur->next->next->tok_string , flag, EOC,T_INT);
															rc = select_col_where_clause(where_col_list, tab , col_list);
															break;
														}

														////////////////////////
														else if (cur->next->next->next->tok_value == K_ORDER)
														{
															add_to_where_list(&where_col_list ,cur->tok_string, cur->next->tok_value,cur->next->next->tok_string , flag, EOC,T_INT);
															//order by for select * without where clause 
															printf("entering ORDER BY NUPUR\n");

															cur = cur->next->next->next->next;

															printf("%s:",cur->tok_string);
															if (cur->tok_value == K_BY)
															{
																cur = cur ->next;
																if (cur->tok_class == identifier)
																{
																											
																	if( get_col_from_table(cur->tok_string,tab) == 0)
																	{
																		strcpy(col_order_name ,cur->tok_string);
																		col_order_type = get_colType_from_table(cur->tok_string, tab);

																		cur =cur->next;

																		if(cur->tok_value==K_DESC)
																		{
																			desc =1;
																			printf("Found Descending\n");
																			cur= cur->next;

																			if(cur->tok_value == EOC)
																			{
																				//call order by
																				printf("Parsing SUCC\n");
																				rc = select_order_by(where_col_list ,col_list, tab, col_order_name, col_order_type, desc);

																			}
																			else
																			{
																				rc= INVALID_SELECT_STATEMENT;
																				cur->tok_value = INVALID;
																				return rc;

																			}								  
																		}
																		else if (cur->tok_value == EOC )
																		{
																			desc = 0;
																			printf("Found Ascending\n");
																			//call order by
																			printf("Parsing SUCC\n");
																			rc = select_order_by(where_col_list ,col_list, tab, col_order_name, col_order_type, desc);

																		}

																		else
																		{
																			rc= INVALID_SELECT_STATEMENT;
																			cur->tok_value = INVALID;
																			return rc;

																		}

																	}
																	else
																	{
																		rc= COL_NOT_EXIST;
																		cur->tok_value = INVALID;
																		return rc;

																	}

																}

																else
																{
																	rc= INVALID_COLUMN_NAME;
																	cur->tok_value = INVALID;
																	return rc;

																}

															}
															else
															{
																rc = INVALID_ORDER_BY;
																cur->tok_value = INVALID;
																return rc;

															}

															break;
														} //order by

														/////////////////////////
														else
														{

															printf("eoc or AND/OR missing, not ending properly\n");
															//error
															rc = INVALID_WHERE_CLAUSE;
															cur->next->next->next->tok_value = INVALID;
															break;
														}
													}

												} //t-int
												else
												{
													printf("DATA MISMATCH\n");
													rc = DATATYPE_MISMATCH_ERROR;
													cur->next->next->tok_value = INVALID;
													return rc;

												}

											}// int-literal

											else
											{

												printf("constant not correct/present\n");
												rc = INVALID_WHERE_CLAUSE;
												cur->next->next->tok_value = INVALID;
												break;
												//error
											}


										}

										else if( (cur->next->tok_value == K_IS) && (cur->next->next->tok_value == K_NULL) )
										{
											flag = 1;
											printf("Found is null condition\n");

											if( (cur->next->next->next->tok_value == K_AND) || (cur->next->next->next->tok_value == K_OR) )
											{

												printf("AND OR present\n");
												add_to_where_list(&where_col_list ,cur->tok_string,0,"", flag ,cur->next->next->next->tok_value ,0);							

												cur = cur->next->next->next;
											}

											else
											{


												if(cur->next->next->next->tok_value == EOC)
												{
													printf("EOC present\n");

													add_to_where_list(&where_col_list ,cur->tok_string, 0 ,"",flag, EOC ,0);
													rc = select_col_where_clause(where_col_list,tab,col_list);
													break;
												}
												//////////////
												else if (cur->next->next->next->tok_value == K_ORDER)
												{
													add_to_where_list(&where_col_list ,cur->tok_string, 0 ,"",flag, EOC ,0);
													//order by for select * without where clause 
													printf("entering ORDER BY NUPUR\n");

													cur = cur->next->next->next->next;

													printf("%s:",cur->tok_string);
													if (cur->tok_value == K_BY)
													{
														cur = cur ->next;
														if (cur->tok_class == identifier)
														{
															if( get_col_from_table(cur->tok_string,tab) == 0)
															{
																strcpy(col_order_name ,cur->tok_string);
																col_order_type = get_colType_from_table(cur->tok_string, tab);
																cur =cur->next;

																if(cur->tok_value==K_DESC)
																{
																	desc =1;
																	printf("Found Descending\n");
																	cur= cur->next;

																	if(cur->tok_value == EOC)
																	{
																		//call order by
																		printf("Parsing SUCC\n");
																		rc = select_order_by(where_col_list ,col_list, tab, col_order_name, col_order_type, desc);

																	}
																	else
																	{
																		rc= INVALID_SELECT_STATEMENT;
																		cur->tok_value = INVALID;
																		return rc;

																	}								  
																}
																else if (cur->tok_value == EOC )
																{
																	desc = 0;
																	printf("Found Ascending\n");
																	//call order by
																	printf("Parsing SUCC\n");
																	rc = select_order_by(where_col_list ,col_list, tab, col_order_name, col_order_type, desc);

																}

																else
																{
																	rc= INVALID_SELECT_STATEMENT;
																	cur->tok_value = INVALID;
																	return rc;

																}

															}
															else
															{
																rc= COL_NOT_EXIST;
																cur->tok_value = INVALID;
																return rc;

															}

														}

														else
														{
															rc= INVALID_COLUMN_NAME;
															cur->tok_value = INVALID;
															return rc;

														}

													}
													else
													{
														rc = INVALID_ORDER_BY;
														cur->tok_value = INVALID;
														return rc;

													}

													break;
												} //order by

												/////////////

												else
												{

													printf("eoc or AND/OR missing, not ending properly\n");
													//error
													rc = INVALID_WHERE_CLAUSE;
													cur->next->next->next->tok_value = INVALID;
													break;
												}
											}
										}

										else if( (cur->next->tok_value == K_IS) && (cur->next->next->tok_value == K_NOT) && (cur->next->next->next->tok_value == K_NULL) )
										{
											flag = 0;
											printf("Found is NOT null condition\n");

											if( (cur->next->next->next->next->tok_value == K_AND) || (cur->next->next->next->next->tok_value == K_OR) )
											{
												printf("AND OR present\n");
												add_to_where_list(&where_col_list ,cur->tok_string, 0," ",flag ,cur->next->next->next->next->tok_value, 0);

												cur = cur->next->next->next->next;
											}

											else
											{

												if(cur->next->next->next->next->tok_value == EOC)
												{
													printf("EOC present\n");
													add_to_where_list(&where_col_list ,cur->tok_string,0,"",flag , EOC,0  );
													rc = select_col_where_clause(where_col_list,tab,col_list);

													break;
												}
												////////////
												else if (cur->next->next->next->next->tok_value == K_ORDER)
												{
													add_to_where_list(&where_col_list ,cur->tok_string,0,"",flag , EOC,0  );
													//order by for select * without where clause 
													printf("entering ORDER BY NUPUR\n");

													cur = cur->next->next->next->next->next;

													printf("%s:",cur->tok_string);
													if (cur->tok_value == K_BY)
													{
														cur = cur ->next;
														if (cur->tok_class == identifier)
														{
															if( get_col_from_table(cur->tok_string,tab) == 0)
															{
																strcpy(col_order_name ,cur->tok_string);
																col_order_type = get_colType_from_table(cur->tok_string, tab);
																cur =cur->next;

																if(cur->tok_value==K_DESC)
																{
																	desc =1;
																	printf("Found Descending\n");
																	cur= cur->next;

																	if(cur->tok_value == EOC)
																	{
																		//call order by
																		printf("Parsing SUCC\n");
																		rc = select_order_by(where_col_list ,col_list, tab, col_order_name, col_order_type, desc);

																	}
																	else
																	{
																		rc= INVALID_SELECT_STATEMENT;
																		cur->tok_value = INVALID;
																		return rc;

																	}								  
																}
																else if (cur->tok_value == EOC )
																{
																	desc = 0;
																	printf("Found Ascending\n");
																	//call order by
																	printf("Parsing SUCC\n");
																	rc = select_order_by(where_col_list ,col_list, tab, col_order_name, col_order_type, desc);

																}

																else
																{
																	rc= INVALID_SELECT_STATEMENT;
																	cur->tok_value = INVALID;
																	return rc;

																}

															}
															else
															{
																rc= COL_NOT_EXIST;
																cur->tok_value = INVALID;
																return rc;

															}

														}

														else
														{
															rc= INVALID_COLUMN_NAME;
															cur->tok_value = INVALID;
															return rc;

														}

													}
													else
													{
														rc = INVALID_ORDER_BY;
														cur->tok_value = INVALID;
														return rc;

													}

													break;
												} //order by

												///////////
												else
												{

													printf("eoc or AND/OR missing, not ending properly\n");
													//error
													rc = INVALID_WHERE_CLAUSE;
													cur->next->next->next->next->tok_value = INVALID;
													break;
												}
											}

										}

										else
										{

											printf("Not a valid operation defined after column name\n");
											rc = INVALID_WHERE_CLAUSE;
											cur->tok_value = INVALID;
											break;
										}							  


									}//else

								} //else

								cur = cur->next;		
								printf("Just before loop:%s\n",cur->tok_string);

							} //for



						}//else for valid col name

						//------------------------------------------------------------------------------------------					
					} // where clause

					else if(cur->tok_value == K_ORDER)
					{
						for( j =1; j <=i; j++ )
						{
							//	printf("%s\n",cur2->tok_string);
							int  x = get_col_from_table(cur2->tok_string,tab_entry->table_name);

							if (x == 0 )
							{
								printf("Found col%d\n", j);
								add_to_colList(&col_list,cur2->tok_string);

							}
							else
							{
								printf("didnt Find col NUPUR%d\n",j);
								rc = COL_NOT_EXIST;
								cur2->tok_value= INVALID;
								return rc;
								//break;
							}					
							cur2 = cur2->next->next;

						}//for2 ends here 

						if (j-1 == i)
						{
							printf("you found all the columns , lets select the values from table\n");

							//call function to display
							//int x = select_col_from_file(col_list, tab);
						}
						//order by for select * without where clause 

						printf("entering ORDER BY\n");

						cur = cur->next;
						if (cur->tok_value == K_BY)
						{
							cur = cur ->next;
							if (cur->tok_class == identifier)
							{
								if( get_col_from_table(cur->tok_string,tab) == 0)
								{
									strcpy(col_order_name ,cur->tok_string);
									col_order_type = get_colType_from_table(cur->tok_string, tab);
									cur =cur->next;

									if(cur->tok_value==K_DESC)
									{
										desc =1;
										printf("Found Descending\n");
										cur= cur->next;

										if(cur->tok_value == EOC)
										{
											//call order by
											printf("Parsing SUCC\n");
											rc = select_order_by(NULL ,col_list, tab, col_order_name, col_order_type, desc);


										}
										else
										{
											rc= INVALID_SELECT_STATEMENT;
											cur->tok_value = INVALID;
											return rc;

										}								  
									}
									else if (cur->tok_value == EOC )
									{
										desc = 0;
										printf("Found Ascending\n");
										//call order by
										printf("Parsing SUCC\n");
										rc = select_order_by(NULL ,col_list, tab, col_order_name, col_order_type, desc);

									}

									else
									{
										rc= INVALID_SELECT_STATEMENT;
										cur->tok_value = INVALID;
										return rc;

									}

								}
								else
								{
									rc= COL_NOT_EXIST;
									cur->tok_value = INVALID;
									return rc;

								}


							}

							else
							{
								rc= INVALID_COLUMN_NAME;
								cur->tok_value = INVALID;
								return rc;

							}

						}
						else
						{
							rc = INVALID_ORDER_BY;
							cur->tok_value = INVALID;
							return rc;

						}
					} //order by

					else
					{
						printf("Select Not ending properly\n");
						rc = INVALID_SELECT_STATEMENT;
						cur->tok_value = INVALID;

					}


				} //table exists

			} //valid table name

			else
			{
				printf("NO table name\n");
				rc = INVALID_SELECT_STATEMENT;
				cur->tok_value= INVALID;
			}
		}

		else
		{

			rc = INVALID_SELECT_STATEMENT;
			printf(" something missing\n");

		}

		//free(col_list);

	}//else if

	else if(cur->tok_class == function_name)
	{
		printf("ENTERING  SELECT WITH AGGREGATE\n");
		int star = 0;
		int c_type;
		char col_name[MAX_TOK_LEN];
		int aggregate;
		int x;

		if( (cur->tok_value == F_AVG) ||  (cur->tok_value == F_SUM) || (cur->tok_value == F_COUNT ))
		{
			aggregate = cur->tok_value;
			//cur = cur->next;

			if(aggregate == F_COUNT)
			{
				if(cur->next->tok_value == S_LEFT_PAREN)
				{
					//cur=cur->next;

					if( (cur->next->next->tok_value != S_STAR) && (cur->next->next->tok_class !=identifier ) )
					{
						//error
						printf("Not a valid Count statement");
						rc = INVALID_AGGREGATE_FUNCT;
						cur->next->next->tok_value = INVALID;
						return rc;

					}

					else
					{ 
						if(cur->next->next->tok_value == S_STAR)
						{
							star = 1;

						} //for star symbol

						else
						{
							//if its col name,check it should be an int type
							star = 0;					
							strcpy(col_name,cur->next->next->tok_string);

						} //not star
						//cur = cur->next;
						if(cur->next->next->next->tok_value== S_RIGHT_PAREN)
						{
							printf("found right paranthesis\n");

						}

						else
						{
							printf("Right paranthesis missing");
							rc = INVALID_AGGREGATE_FUNCT;
							cur->next->next->next->tok_value= INVALID;
							return rc;


						}

					}


				} //left paran

				else
				{
					printf("left paranthesis missing");
					rc = INVALID_AGGREGATE_FUNCT;
					cur->next->tok_value= INVALID;
					return rc;

				}
			}//count

			else if( (aggregate == F_AVG ) || (aggregate == F_SUM) )
			{
				if(cur->next->tok_value == S_LEFT_PAREN)
				{
					//cur = cur->next;
					if(cur->next->next->tok_class !=identifier)
					{
						printf("Not a valid SUM/AVG statement");
						rc = INVALID_AGGREGATE_FUNCT;
						cur->next->next->tok_value= INVALID;
						return rc;

					}

					else
					{
						strcpy(col_name,cur->next->next->tok_string);
						//cur = cur->next;
						if(cur->next->next->next->tok_value== S_RIGHT_PAREN)
						{
							printf("found right paranthesis\n");

						}
						else
						{
							printf("Right paranthesis missing");
							rc = INVALID_AGGREGATE_FUNCT;
							cur->next->next->next->tok_value= INVALID;
							return rc;

						}


					}//col name
				}//left-paran

				else
				{
					printf("left paranthesis missing");
					rc = INVALID_AGGREGATE_FUNCT;
					cur->next->tok_value= INVALID;
					return rc;

				}

			}//aggregate-Sum/avg

			//cur =cur->next;

			if (cur->next->next->next->next->tok_value != K_FROM)
			{
				printf("From not present\n");
				rc = INVALID_SELECT_STATEMENT;
				cur->next->next->next->next->tok_value = INVALID;

			}

			else
			{
				//cur = cur->next;

				tab = cur->next->next->next->next->next->tok_string;
				tab_entry = get_tpd_from_list(tab);


				if( tab_entry == NULL)
				{

					printf("table name not valid\n");
					rc= TABLE_NOT_EXIST;
					cur->next->next->next->next->next->tok_value= INVALID;

				}

				else
				{
					//check the col name
					if(star == 0)
					{
						x = get_col_from_table(col_name,tab);

						if (x == 0)
						{				
							printf("valid column name:%s\n",col_name);				 
						}
						else
						{
							printf("Column does not exist:%s\n",col_name);
							rc = COL_NOT_EXIST;
							cur->next->next->tok_value = INVALID;
							return rc;						
						}
					}
					if ( (aggregate==F_SUM) || (aggregate==F_AVG) )
					{
						c_type = get_colType_from_table(col_name, tab);
						if(c_type == T_CHAR)
						{
							printf("cant apply on character type\n");
							rc = INVALID_AGGREGATE_FUNCT;
							cur->next->next->tok_value = INVALID;
							return rc;							
						}
						else
						{
							//fouund integer column
							printf("Found integer col for sum/avgn\n");

						}				  
					}


				} // if table name valid

			}//from

			/*-------------------------------------------where clause starts here--------------------------------------------------*/
			cur = cur->next->next->next->next->next->next;
			printf("expects where: %s\n",cur->tok_string);

			if(cur->tok_value == K_WHERE)
			{
				printf("Found WHERE\n");
				cur= cur->next;

				if ((cur->tok_class != keyword) &&
					(cur->tok_class != identifier) &&
					(cur->tok_class != type_name))
				{
					// Error
					printf("Expecting a valid column string after WHERE\n");
					rc = INVALID_WHERE_CLAUSE;
					cur->tok_value = INVALID;
					
				}

				else
				{


					for( int i=1 ; i <= MAX_NUM_COL ; i++)
					{

						int flag = -1;
						if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
						{
							printf("Expecting a vaid column string after WHERE\n");
							rc = INVALID_WHERE_CLAUSE;
							cur->tok_value = INVALID;
							break;


						}

						else
						{

							int  y = get_col_from_table(cur->tok_string,tab_entry->table_name);

							if (y != 0)
							{

								printf("column not found in table file\n");
								rc = COL_NOT_EXIST;
								cur->tok_value = INVALID;
								break;
							}

							else
							{
								int col_type;
								col_type = get_colType_from_table(cur->tok_string,tab_entry->table_name);		

								printf("Found column\n");								
								if( (cur->next->tok_value == S_EQUAL) || (cur->next->tok_value == S_LESS) || (cur->next->tok_value == S_GREATER) )
								{
									printf("Some Symbol present\n");

									if(cur->next->next->tok_value == STRING_LITERAL)
									{
										if(col_type == T_CHAR)
										{
											if( (cur->next->tok_value == S_EQUAL) || (cur->next->tok_value == S_LESS) || (cur->next->tok_value == S_GREATER) )
											
											{
												printf("String present\n");
												if( (cur->next->next->next->tok_value == K_AND) || (cur->next->next->next->tok_value == K_OR) )
												{
													printf("AND/OR present after string\n");

													add_to_where_list(&where_col_list ,cur->tok_string, cur->next->tok_value,cur->next->next->tok_string ,flag, cur->next->next->next->tok_value,T_CHAR);																				
													cur = cur->next->next->next;
												}

												else
												{

													if(cur->next->next->next->tok_value == EOC)
													{
														printf("EOC present after String\n");

														add_to_where_list(&where_col_list ,cur->tok_string, cur->next->tok_value,cur->next->next->tok_string , flag, EOC,T_CHAR);
														rc = select_aggregate(where_col_list,tab,star,aggregate,col_name);
														break;
													}

													else
													{

														printf("eoc or AND/OR missing, not ending properly\n");
														//error
														rc = INVALID_WHERE_CLAUSE;
														cur->next->next->next->tok_value = INVALID;
														break;
													}
												}
											} //symbol check for string

											else
											{
												printf("Symbol for string is not accepted\n");
												rc = INVALID_SYMBOL;
												cur->next->tok_value = INVALID;
												break;
											}
										}
										else
										{
											printf("DATA MISMATCH\n");
											rc = DATATYPE_MISMATCH_ERROR;
											cur->next->next->tok_value = INVALID;
											return rc;


										}

									}//string literal

									else if(cur->next->next->tok_value == INT_LITERAL)
									{
										if( col_type== T_INT)
										{
											printf("Numeric present\n");
											if( (cur->next->next->next->tok_value == K_AND) || (cur->next->next->next->tok_value == K_OR) )
											{
												printf("AND/OR present after numeric\n");

												add_to_where_list(&where_col_list ,cur->tok_string, cur->next->tok_value,cur->next->next->tok_string ,flag, cur->next->next->next->tok_value,T_INT);							

												cur = cur->next->next->next;
												//printf("%s::",cur->tok_string);
											}

											else
											{

												if(cur->next->next->next->tok_value == EOC)
												{
													printf("EOC present after numeric\n");

													add_to_where_list(&where_col_list ,cur->tok_string, cur->next->tok_value,cur->next->next->tok_string , flag, EOC,T_INT);
													rc = select_aggregate(where_col_list,tab,star,aggregate,col_name);
													break;
												}

												else
												{

													printf("eoc or AND/OR missing, not ending properly\n");
													//error
													rc = INVALID_WHERE_CLAUSE;
													cur->next->next->next->tok_value = INVALID;
													break;
												}
											}
										}//T-int
										else
										{
											printf("DATA MISMATCH\n");
											rc = DATATYPE_MISMATCH_ERROR;
											cur->next->next->tok_value = INVALID;
											return rc;

										}

									}

									else
									{

										printf("constant not correct/present\n");
										rc = INVALID_WHERE_CLAUSE;
										cur->next->next->tok_value = INVALID;
										break;
										//error
									}


								}

								else if( (cur->next->tok_value == K_IS) && (cur->next->next->tok_value == K_NULL) )
								{
									flag = 1;
									printf("Found is null condition\n");

									if( (cur->next->next->next->tok_value == K_AND) || (cur->next->next->next->tok_value == K_OR) )
									{

										printf("AND OR present\n");
										add_to_where_list(&where_col_list ,cur->tok_string,0,"", flag ,cur->next->next->next->tok_value ,0);							

										cur = cur->next->next->next;
									}

									else
									{


										if(cur->next->next->next->tok_value == EOC)
										{
											printf("EOC present\n");

											add_to_where_list(&where_col_list ,cur->tok_string, 0 ,"",flag, EOC ,0);
											rc = select_aggregate(where_col_list,tab,star,aggregate,col_name);
											break;
										}

										else
										{

											printf("eoc or AND/OR missing, not ending properly\n");
											//error
											rc = INVALID_WHERE_CLAUSE;
											cur->next->next->next->tok_value = INVALID;
											break;
										}
									}
								}

								else if( (cur->next->tok_value == K_IS) && (cur->next->next->tok_value == K_NOT) && (cur->next->next->next->tok_value == K_NULL) )
								{
									flag = 0;
									printf("Found is NOT null condition\n");

									if( (cur->next->next->next->next->tok_value == K_AND) || (cur->next->next->next->next->tok_value == K_OR) )
									{
										printf("AND OR present\n");
										add_to_where_list(&where_col_list ,cur->tok_string, 0," ",flag ,cur->next->next->next->next->tok_value, 0);

										cur = cur->next->next->next->next;
									}

									else
									{

										if(cur->next->next->next->next->tok_value == EOC)
										{
											printf("EOC present\n");
											add_to_where_list(&where_col_list ,cur->tok_string,0,"",flag,EOC,0  );
											rc = select_aggregate(where_col_list,tab,star,aggregate,col_name);

											break;
										}

										else
										{

											printf("eoc or AND/OR missing, not ending properly\n");
											//error
											rc = INVALID_WHERE_CLAUSE;
											cur->next->next->next->next->tok_value = INVALID;
											break;
										}
									}

								}

								else
								{

									printf("Not a valid operation defined after column name\n");
									rc = INVALID_WHERE_CLAUSE;
									cur->tok_value = INVALID;
									break;
								}							  


							}//else

						} //else

						cur = cur->next;		
						printf("Just before loop:%s\n",cur->tok_string);

					} //for

				}//else for valid col name

			} //if for where clause


			else if(cur->tok_value== EOC)
			{
				printf("NO where condition for aggregate function \n");

				rc = select_aggregate(NULL,tab,star,aggregate,col_name);
			}

			else if(cur->tok_value == K_ORDER && cur->next->tok_value == K_BY)
			{
				printf("ORDER BY NOT EXPECTED IN AGGREGATE \n");
				rc= INVALID_SELECT_STATEMENT;
				cur->tok_value = INVALID;
				return rc;

			}

			else
			{
				printf("Select Not ending correctly \n");
				rc= INVALID_SELECT_STATEMENT;
				cur->tok_value = INVALID;
				return rc;

			}

			/*-------------------------------------------where clause ends here--------------------------------------------------*/

		}//count,sum,avg
		else
		{
			printf("Not a valid aggregate function\n");
			rc = INVALID_AGGREGATE_FUNCT;
			cur->tok_value = INVALID;
			return rc;

		}

	}//if for aggregate func

	//if anything else other than * or aggagate or colname

	else
	{
		printf("Error in select statement\n");
		rc = INVALID_SELECT_STATEMENT;
		cur->tok_value = INVALID;

	}

	//printf("just before returning\n");
	//printf("%d\n",rc);
	//printf("just before returning\n");
	return rc;

} // ends sem_select


int  select_from_file(char *tab)

{
	int rc =0;
	char filename[20] ;
	strcpy(filename , tab);
	strcat(filename, ".tab");
	//	printf("%s\n",filename);

	tpd_entry *tab_entry;
	tab_entry = get_tpd_from_list(tab);

	//first get the table and open that corresponding file

	FILE *fp;
	fp = fopen(filename, "rbc");
	int i = 1;

	struct _stat file_stat;
	_fstat(_fileno(fp), &file_stat);

	//printf("%s size is: %d\n",filename ,file_stat.st_size);


	if(fp == NULL)
	{

		printf("\nfile error\n");

		rc = FILE_OPEN_ERROR;

	}

	else
	{

		//first read the num of records from ur header

		table_file_header *header;
		//start reading ur data

		header = (table_file_header*)calloc(1, sizeof(table_file_header));
		fread(header ,sizeof(table_file_header) ,1,fp);
		cd_entry *col_entry;

		int numCol;

		int j;

		numCol = tab_entry->num_columns;

		fseek(fp,sizeof(table_file_header), SEEK_SET);
		
	

		/*
		for(i=1; i <= numCol ; i++ )
		{
			printf("%s\t\t",col_entry->col_name);
			col_entry++;
		}
		*/
		//for displaying column name


		char col_header_name[MAX_IDENT_LEN];
		int header_len;
		int col_max_len;
		int total_header_len =0;

		col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

		for(i=1; i <= numCol ; i++ )
		{		
			strcpy(col_header_name,col_entry->col_name);
			header_len = strlen(col_header_name);
			col_max_len = col_entry->col_len;
			
			if(header_len>col_max_len)
			col_max_len=header_len;

			if(col_entry->col_type== T_CHAR)
			{
				printf("%*s|",-col_max_len,col_header_name);
				total_header_len+=(col_max_len+1);
			}
			else
			{
				printf("%10s|",col_header_name);
				total_header_len+=(10+1);
			}
			col_entry++;

		}

		
		printf("\n");
		for( int n=0;n<total_header_len;n++)
		printf("-");
		printf("\n");
		
		//printf("\n--------------------------------------------------------------------------------\n");

        
		for(j= 1; j <=header->num_records; j++)
		{

			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			//	printf("enter 1 for\n");

			for(i=1; i <= numCol ; i++ )
			{		
				strcpy(col_header_name,col_entry->col_name);
				header_len = strlen(col_header_name);
				col_max_len = col_entry->col_len;

				if(header_len>col_max_len)
				col_max_len=header_len;

				if(col_entry->col_type == T_CHAR)
				{

					char buffer1[MAX_TOK_LEN] ;
					memset(buffer1, 0, sizeof(buffer1));
					char m;
					fread(&m,1,1,fp);

					if( m == 0)
					{									
						fread(buffer1 , sizeof(char)*col_entry->col_len  , 1 , fp);
						//printf("-\t\t");
						char j = '-';
						printf("%*c|",-col_max_len,j);
					}
					else
					{
						fread(buffer1 , sizeof(char)*(col_entry->col_len)  , 1 , fp);
						//printf("%s\t\t",buffer1);
						printf("%*s|",-col_max_len,buffer1);
					}

				}

				if(col_entry->col_type== T_INT)
				{

					char m;
					fread(&m, 1,1,fp);

					if(m == 0)
					{
						int *p;
						p = (int*) calloc(sizeof(int),1);
						fread( p,sizeof(int),1,fp);
						//printf("-\t\t");
						char j = '-';
						printf("%10c|",j);

					}
					else
					{
						int *p;

						p = (int*) calloc(sizeof(int),1);
						fread( p, sizeof(int),1,fp );
						//printf("%d\t\t",*p);
						printf("%10d|",*p);
					}
				}

				col_entry++;
			} //for col

			printf("\n");
		}//for rows

		printf("\n%s size is: %d\n",filename ,file_stat.st_size);

		fflush(fp);
		fclose(fp);

	}

	return rc;
} // select from file ends

int select_all_where_clause(where_list *where_col_list, char *tab)
{
	printf("entering selct all for where clause\n");

	where_col_list = where_col_list->next;
	
	int rc =0;
	char filename[20] ;
	strcpy(filename , tab);
	strcat(filename, ".tab");
	//	printf("%s\n",filename);

	tpd_entry *tab_entry = NULL;
	tab_entry = get_tpd_from_list(tab);

	//first get the table and open that corresponding file

	FILE *fp;
	fp = fopen(filename, "rbc");
	int i = 1;

	struct _stat file_stat;
	_fstat(_fileno(fp), &file_stat);

	//printf("%s size is: %d\n",filename ,file_stat.st_size);


	if(fp == NULL)
	{

		printf("\nfile error\n");

		rc = FILE_OPEN_ERROR;

	}

	else
	{

		//first read the num of records from ur header

		table_file_header *header=NULL;
		header = (table_file_header*)calloc(1, sizeof(table_file_header));
		fread(header ,sizeof(table_file_header) ,1,fp);

		cd_entry *col_entry = NULL;	

		int numCol;
		int j;
		numCol = tab_entry->num_columns;

		fseek(fp,sizeof(table_file_header), SEEK_SET);
		col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

		//	printf("enter 1 for\n");

		//printf("MAIN FUNCTION\n");

		//	while(where_col_list->next != NULL)
		//	{
		//printf("entering while\n");
		int x =0;
		int rowIndex[100];
		int r = 0;
		int rowIndex2[100];
		int q = 0;
		int finalArr[100];
		int p = 0;
		int rel_op;

		//printf("link list col name :%s\n",where_col_list->col_name);

		for(i = 1; i <= numCol ; i++ )

		{
			//printf("entering for\n");
			if(strcmpi (where_col_list->col_name ,col_entry->col_name) == 0)

			{						
				//printf("col found\n");
				fseek(fp , x, SEEK_CUR);
				int operator_value = where_col_list->arith_operator;
				if (where_col_list->null_check == -1)
				{
					if(where_col_list->col_type == col_entry->col_type)
					{
						if( where_col_list->col_type == T_CHAR )
						{
							//char buffer[MAX_IDENT_LEN];
							char *buffer = (char*)calloc(1, MAX_TOK_LEN);
							//memset(buffer ,'/0' ,sizeof(buffer)); 
							char m;
							
							//fseek(fp,1,SEEK_CUR);
							//printf("Operator Value: %i",operator_value);
							for(j=1; j <=header->num_records; j++)
							{

								//printf("entering for row loop\n");		
								fread(&m,1,1,fp);
								fread(buffer,col_entry->col_len, 1, fp );	
								//printf("%s\n",buffer);

								if(operator_value== S_EQUAL)
								{
									//printf("Found = symbol\n");

									if (m!= 0)
									{
										if(strcmp(buffer, where_col_list->value) == 0)
										{										
											//printf("found data \n");
											rowIndex[r] = j;
											r++;

										}
									}

								}

								else if(operator_value== S_LESS)
								{
									//printf("Found < symbol\n");

									if ( m!=0)
									{
										if(strcmp(buffer, where_col_list->value) < 0)
										{										
											//printf("found data \n");
											rowIndex[r] = j;
											r++;

										}
									}
								
								}


								else if(operator_value== S_GREATER)
								{
									//printf("Found > symbol\n");
									
									if (m != 0)
									{
										if(strcmp(buffer, where_col_list->value) > 0)
										{										
											//printf("found data \n");
											rowIndex[r] = j;
											r++;

										}
									}
															
								}
								
								int new_seek_length = (header->record_size - col_entry->col_len) - 1;
								fseek( fp, new_seek_length, SEEK_CUR);

							} //for rows

							printf("r is:%d\n",r);
							//printf("beofr free\n");

							//printf("after free\n");
							free(buffer);
						}

						else if( where_col_list->col_type == T_INT)

						{

							//printf("numeric data\n");
							int *p;
							p = (int*)calloc(1,sizeof(int));

							char m;


							//fseek(fp,1,SEEK_CUR);

							for(j = 1; j <=header->num_records; j++)
							{
								//printf("entering for row loop\n");		
								fread(&m,1,1,fp);
								fread(p, sizeof(int), 1, fp );	
								//printf("%d\n",*p);

								if(where_col_list->arith_operator== S_EQUAL) 
								{
									//printf("Equal symbol\n");

									if(m !=0)
									{

										if(*p == atoi(where_col_list->value) )
										{										
											//printf("found data \n");
											rowIndex[r] = j;
											r++;

										}
									}
								}

								else if((where_col_list->arith_operator== S_LESS))
								{
									//printf("Less symbol\n");

									if(m !=0)
									{

										if(*p < atoi(where_col_list->value) )
										{										
											//printf("found data \n");
											rowIndex[r] = j;
											r++;

										}
									}

								}
								else if((where_col_list->arith_operator== S_GREATER))
								{
									//printf("More symbol\n");

									if(m != 0)
									{

										if(*p > atoi(where_col_list->value) )
										{										
											//printf("found data \n");
											rowIndex[r] = j;
											r++;

										}

									}
								}

								int new_seek_length = (header->record_size - sizeof(int)) - 1;
								fseek( fp, new_seek_length, SEEK_CUR);

							} //for rows

							printf("r is:%d\n",r);
							//free(p);

						}
					} //data mismatch

					else								
					{							
						printf("data mismatch error\n");
						rc = DATATYPE_MISMATCH_ERROR;
						//break;
						return rc;

					}

				} //null check absent

				else
				{
					if(where_col_list->null_check ==1)
					{
						//printf(" null condition\n");

						//fseek(fp,1,SEEK_CUR);
						char *m;
						m = (char*)calloc(1,sizeof(char));
						for(j=1; j <=header->num_records; j++)
						{
							//printf("entering for row loop\n");								
							fread(m,1, 1, fp );									
							//printf("%d\n",*m);

							if(*m == 0)
							{										
								//printf("found data \n");
								rowIndex[r] = j;
								r++;

							}
							int new_seek_length = (header->record_size - 1);
							fseek( fp, new_seek_length, SEEK_CUR);

						} //for rows

						printf("r is:%d\n",r);
						free(m);



					}					
					else
					{
						//printf(" NOT null condition\n");
						char *m;
						m = (char*)calloc(1,sizeof(char));

						for(j=1; j <=header->num_records; j++)
						{
							//printf("entering for row loop\n");								
							fread(m,1, 1, fp );									
							//printf("%d\n",*m);

							if(*m != 0)
							{										
								//printf("found data \n");
								rowIndex[r] = j;
								r++;

							}
							int new_seek_length = (header->record_size - 1);
							fseek( fp, new_seek_length, SEEK_CUR);

						} //for rows

						printf("r is:%d\n",r);
						free(m);

					}
				} // null check present

				break;

			} //if col name found

			else
			{

				//printf("column not found\n");
				x = x + col_entry->col_len+1;
				col_entry++;
			}

		}//for columns

		//		} // while

		if (where_col_list->relational_operator == EOC)
		{
			//printf("only one where condition\n");

			for (int i = 0 ; i<r ; i++)
			{
				finalArr[p] = rowIndex[i];
				p++;
			}	

		}

		else
		{
			rel_op = where_col_list->relational_operator;

			//printf("Two where condition\n");

			where_col_list=where_col_list->next;

			fseek(fp,sizeof(table_file_header), SEEK_SET);
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

			//printf("entering while\n");
			x =0;
			//int rowIndex2[1000];
			//r = 0;

			//printf("link list col name :%s\n",where_col_list->col_name);
			for(i = 1; i <= numCol ; i++ )

			{
				//printf("entering for\n");
				if(strcmpi (where_col_list->col_name ,col_entry->col_name) == 0)

				{						
					//printf("col found\n");
					fseek(fp , x, SEEK_CUR);

					if (where_col_list->null_check == -1)
					{
						if(where_col_list->col_type == col_entry->col_type)
						{
							if( where_col_list->col_type == T_CHAR )
							{
								//char buffer[MAX_IDENT_LEN];
								//memset(buffer ,0 ,sizeof(buffer)); 
								char * buffer =(char*)calloc(1,MAX_TOK_LEN);
								//fseek(fp,1,SEEK_CUR);
								char m;

								for(j=1; j <=header->num_records; j++)
								{
									//printf("entering for row loop\n");		
									fread(&m,1,1,fp);
									fread(buffer,col_entry->col_len, 1, fp );	
									//printf("%s\n",buffer);

									if(where_col_list->arith_operator== S_EQUAL)
									{
										if( m != 0)
										{
											if(strcmp(buffer, where_col_list->value) == 0)
											{										
												//printf("found data \n");
												rowIndex2[q] = j;
												q++;

											}
										}
									}
									else if(where_col_list->arith_operator== S_LESS)
									{
										if (m != 0)
										{
											if(strcmp(buffer, where_col_list->value) < 0)
											{										
												//printf("found data \n");
												rowIndex2[q] = j;
												q++;

											}
										}

									}

									else if(where_col_list->arith_operator== S_GREATER)
									{
										if (m != 0)
										{
											if(strcmp(buffer, where_col_list->value) > 0)
											{										
												//printf("found data \n");
												rowIndex2[q] = j;
												q++;

											}
										}
									
									}

									int new_seek_length = (header->record_size - col_entry->col_len) - 1;
									fseek( fp, new_seek_length, SEEK_CUR);

								} //for rows

								printf("q is:%d\n",q);
								//printf("beofr free\n");

								//printf("after free\n");
								free(buffer);
							}

							else if( where_col_list->col_type == T_INT)

							{

								//printf("numeric data\n");
								int *p;
								p = (int*)calloc(1,sizeof(int));

								//fseek(fp,1,SEEK_CUR);
								char m;


								for(j=1; j <=header->num_records; j++)
								{
									//printf("entering for row loop\n");
									fread(&m,1,1,fp);
									fread(p, sizeof(int), 1, fp );	
									//printf("%d\n",*p);

									if(where_col_list->arith_operator== S_EQUAL) 
									{
										//printf("Equal symbol\n");

										if(m!=0)
										{

											if(*p == atoi(where_col_list->value) )
											{										
												//printf("found data \n");
												rowIndex2[q] = j;
												q++;

											}
										}
									}

									else if((where_col_list->arith_operator== S_LESS))
									{
										//printf("Less symbol\n");

										if(m!=0)
										{

											if(*p < atoi(where_col_list->value) )
											{										
												//printf("found data \n");
												rowIndex2[q] = j;
												q++;

											}
										}

									}
									else if((where_col_list->arith_operator== S_GREATER))
									{
										//printf("More symbol\n");
										if(m !=0)
										{

											if(*p > atoi(where_col_list->value) )
											{										
												//printf("found data \n");
												rowIndex2[q] = j;
												q++;

											}
										}														
									}

									int new_seek_length = (header->record_size - sizeof(int)) - 1;
									fseek( fp, new_seek_length, SEEK_CUR);

								} //for rows

								printf("q is:%d\n",q);
								free(p);

							}
						} //data mismatch

						else								
						{							
							printf("data mismatch error\n");
							rc = DATATYPE_MISMATCH_ERROR;
							//break;
							return rc;

						}

					} //null check absent

					else
					{
						if(where_col_list->null_check ==1)
						{
							//printf(" null condition\n");

							//fseek(fp,1,SEEK_CUR);
							char *m;
							m = (char*)calloc(1,sizeof(char));
							for(j=1; j <=header->num_records; j++)
							{
								//printf("entering for row loop\n");								
								fread(m,1, 1, fp );									
								//printf("%d\n",*m);

								if(*m == 0)
								{										
									//printf("found data \n");
									rowIndex2[q] = j;
									q++;

								}
								int new_seek_length = (header->record_size - 1);
								fseek( fp, new_seek_length, SEEK_CUR);

							} //for rows

							printf("q is:%d\n",q);
							free(m);



						}					
						else
						{
							//printf(" NOT null condition\n");
							char *m;
							m = (char*)calloc(1,sizeof(char));

							for(j=1; j <=header->num_records; j++)
							{
								//printf("entering for row loop\n");								
								fread(m,1, 1, fp );									
								//printf("%d\n",*m);

								if(*m != 0)
								{										
									//printf("found data \n");
									rowIndex2[q] = j;
									q++;

								}
								int new_seek_length = (header->record_size - 1);
								fseek( fp, new_seek_length, SEEK_CUR);

							} //for rows

							printf("q is:%d\n",q);
							free(m);

						}
					} // null check present

					break;

				} //if col name found

				else
				{

					//printf("column not found\n");
					x = x + col_entry->col_len+1;
					col_entry++;
				}

			}//for columns

		} // 2nd where cond

		if(rel_op == K_OR)
		{
			//printf("before union\n");
			i = 0;
			j = 0;

			while(i < r && j < q)
			{

				if(rowIndex[i] < rowIndex2[j])
				{
					//printf(" %d ", rowIndex[i]);
					finalArr[p] = rowIndex[i];
					i++;
					p++;
				}

				else if(rowIndex2[j] < rowIndex[i])
				{
					//printf(" %d ", rowIndex2[j]);
					finalArr[p] = rowIndex2[j];
					j++;
					p++;

				}
				else
				{
					//printf("enter 3 loop\n");
					//printf(" %d ", rowIndex2[j]);
					finalArr[p] = rowIndex2[j];
					//printf(" %d ", finalArr[p] );
					p++;
					j++;
					i++;
				}
			}

			/* Print remaining elements of the larger array */

			while(i < r)
			{
				//printf(" %d ", rowIndex[i]);
				finalArr[p] = rowIndex[i];
				//printf(" %d ", finalArr[p] );
				i++;
				p++;

			}
			while(j < q)
			{

				//printf(" %d ", rowIndex2[j]);
				finalArr[p] = rowIndex2[j];
				//printf(" %d ", finalArr[p] );
				j++;
				p++;
			}

			//printf("after union\n");

/*
			for(int u=0;u<p;u++)
			{	 
				printf("printing union array\n");
				printf("%d:",finalArr[u]);
			}	
*/
		} // rel oper for OR


		if (rel_op == K_AND)
		{
			int i = 0, j = 0;
			while ((i < r) && (j < q))
			{    
				if (rowIndex[i] < rowIndex2[j])
				{
					i++;
				}

				else if (rowIndex[i] > rowIndex2[j])
				{
					j++;
				}

				else
				{
					finalArr[p] = rowIndex[i];
					i++;
					j++;
					p++;
				}
			}		

			for(int u=0;u<p;u++)
			{	 
				printf("printing intersection array\n");
				printf("%d:",finalArr[u]);
			}


		} // for rel oper AND

		/* select the data */
		printf("Performing select * with where clause\n");
		printf("p : %d\n",p);

		/*
		col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
		for(i=1; i <= numCol ; i++ )
		{
			printf("%s\t\t",col_entry->col_name);
			col_entry++;
		}

		printf("\n--------------------------------------------------------------------------------\n");
		*/

		char col_header_name[MAX_IDENT_LEN];
		int header_len;
		int col_max_len;
		int total_header_len =0;

		col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

		for(i=1; i <= numCol ; i++ )
		{		
			strcpy(col_header_name,col_entry->col_name);
			header_len = strlen(col_header_name);
			col_max_len = col_entry->col_len;
			
			if(header_len>col_max_len)
			col_max_len=header_len;

			if(col_entry->col_type== T_CHAR)
			{
				printf("%*s|",-col_max_len,col_header_name);
				total_header_len+=(col_max_len+1);
			}
			else
			{
				printf("%10s|",col_header_name);
				total_header_len+=(10+1);
			}
			col_entry++;

		}

		
		printf("\n");
		for( int n=0;n<total_header_len;n++)
		printf("-");
		printf("\n");


////////////////////////////////////////////////////////////////

		if (p == 0)
		{
			printf("Found NO rows based on WHERE condition\n");
			//printf("\nXX\n");
			return rc;
		
		}

		for(j = 1 ; j <= p ; j++)
		{

			fseek(fp, (sizeof(table_file_header) + (header->record_size)*(finalArr[j-1]-1)) ,SEEK_SET);
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			//	printf("enter 1 for\n");

			for(i=1; i <= numCol ; i++ )
			{
				strcpy(col_header_name,col_entry->col_name);
				header_len = strlen(col_header_name);
				col_max_len = col_entry->col_len;

				if(header_len>col_max_len)
				col_max_len=header_len;

				if(col_entry->col_type == T_CHAR)

				{

					char buffer1[MAX_TOK_LEN] ;
					memset(buffer1, '\0', sizeof(buffer1));
					char l;
					//l = (char*)calloc(1,sizeof(char));

					fread(&l,1,1,fp);
					//printf("l:%d:",l);

					if( l == 0)
					{
						//printf("m is o\n");
						fread(buffer1 , sizeof(char)*col_entry->col_len  , 1 , fp);
						//printf("-\t\t");
						char j = '-';
						printf("%*c|",-col_max_len,j);


					}
					else
					{
						//printf("m is not o");
						fread(buffer1 , sizeof(char)*(col_entry->col_len)  , 1 , fp);
						//printf("%s\t\t",buffer1);
						printf("%*s|",-col_max_len,buffer1);
					}

				}

				if(col_entry->col_type== T_INT)
				{

					char l;
					fread(&l, 1,1,fp);

					if(l == 0)
					{
						int *b;
						b = (int*) calloc(sizeof(int),1);
						fread( b,sizeof(int),1,fp);
						//printf("-\t\t");
						char j = '-';
						printf("%10c|",j);
						free(b);

					}

					else
					{
						int *b;

						b = (int*) calloc(sizeof(int),1);
						fread( b, sizeof(int),1,fp );
						//printf("%d\t\t",*b);
						printf("%10d|",*b);
						free(b);

					}
				}

				col_entry++;
			} //for columns

			printf("\n");

		}//for rows

	//	printf("FF\n");

		free(header);
	//	printf("Header\n");
	} //file oper

	//printf("FF\n");
	fflush(fp);
	fclose(fp);
	//printf("FFii\n");

	//printf("rcHERE:%d\n",rc);

	return rc;

}

int get_col_from_table( char *col, char *tab)
{
	int a = -1;

	tpd_entry *tab_entry;
	tab_entry = get_tpd_from_list(tab);

	cd_entry *col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);


	for(int i=1; i<= tab_entry->num_columns; i++,col_entry++)
	{
		//printf("entering for\n");
		if (strcmpi(col_entry->col_name, col) == NULL )

		{			
			//printf("col found\n");
			a = 0;
			break;

		}
		else
		{
			a = 1;

		}


	}

	return a;
}

int get_colType_from_table( char *col, char *tab)
{
	int col_type;

	tpd_entry *tab_entry;
	tab_entry = get_tpd_from_list(tab);

	cd_entry *col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);


	for(int i = 1; i<= tab_entry->num_columns; i++,col_entry++)
	{
		//printf("entering for\n");
		if (strcmpi(col_entry->col_name, col) == NULL )
		{			
			//printf("col found\n");

			col_type = col_entry->col_type;
			break;

		}

	}

	return col_type;
}


int select_col_from_file(column_list *col_list , char *tab)

{
	int rc = 0;
	char filename[50] ;
	strcpy(filename , tab);
	strcat(filename, ".tab");
	//	printf("%s\n",filename);

	tpd_entry *tab_entry;
	tab_entry = get_tpd_from_list(tab);

	//first get the table and open that corresponding file

	FILE *fp;
	fp = fopen(filename, "rbc");
	int i = 1;

	if(fp == NULL)
	{
		printf("\nfile error\n");
		rc = FILE_OPEN_ERROR;
	}

	else
	{
		//first read the num of records from ur header

		table_file_header *header;
		//start reading ur data

		header = (table_file_header*)calloc(1, sizeof(table_file_header));
		fread(header ,sizeof(table_file_header) ,1,fp);
		printf("Number of records:%d\n\n",header->num_records);

		cd_entry *col_entry;

		col_list =col_list->next;

		column_list *cl;
		cl = col_list;

		column_list *cl2;
		cl2 = col_list;


		while(cl->next != NULL)
		{
			printf("%s\t\t",cl->col_name);
			cl =cl->next;
		}	
		printf("%s\t\t",cl->col_name);

		printf("\n--------------------------------------------------------------------------\n");


		//now start reading these values from file

		for(int r = 0 ; r < header->num_records ;r++)
		{
			fseek(fp, sizeof(table_file_header)+ (r)*(header->record_size) ,SEEK_SET);
			col_list = cl2;
			int x = 0;
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

			while(col_list->next != NULL)
			{
				//	printf("entering while\n");	
				//	printf("\nname of the col paased in select:%s\n",col_list->col_name);

				x = 0;
				col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
				fseek(fp, sizeof(table_file_header)+ (r)*(header->record_size) ,SEEK_SET);


				for (i=0 ; i< tab_entry->num_columns; i++ )
				{
					//	printf("\n inside for: actual col name: %s",col_entry->col_name);
					//	printf("\n Comparing %s and %s the value is %d",col_list->col_name,col_entry->col_name,strcmpi( col_entry->col_name, col_list->col_name ));

					if (strcmpi( col_entry->col_name, col_list->col_name )== 0)
					{

						//printf("\n inside if : actual col name: %s",col_entry->col_name);
						//printf("\n inside if: actual col type: %d\n",col_entry->col_type);
						//read this value from file

						if(col_entry->col_type == T_CHAR)
						{							
							char buffer[MAX_TOK_LEN];
							memset(buffer,0,sizeof(buffer));						
							fseek(fp,x,SEEK_CUR);

							//fseek(fp,1,SEEK_CUR);

							char m;
							fread(&m,sizeof(char),1,fp);

							if (m == 0)
							{
								fread(buffer,sizeof(char)*col_entry->col_len,1,fp);
								printf("-\t\t");
								col_entry++;
								break;

							}

							else
							{
								fread(buffer,sizeof(char)*col_entry->col_len,1,fp);
								printf("%s\t\t",buffer);
								col_entry++;
								break;

							}

						}

						else
						{	int *p;
						p = (int*)calloc(1,sizeof(int));
						fseek(fp,x,SEEK_CUR);
						//	fseek(fp,1,SEEK_CUR);

						char m;
						fread(&m,1,1,fp);

						if(m == 0)
						{
							fread(p,sizeof(int),1,fp);
							printf("-\t\t");
							col_entry++;
							free(p);
							break;

						}

						else
						{
							fread(p,sizeof(int),1,fp);
							printf("%d\t\t",*p);
							col_entry++;
							free(p);
							break;

						}


						}

					}//if col is there 


					// else move to next col
					else	
					{
						//printf("entering else if no match\n");				
						x += (col_entry->col_len +1 );
						col_entry++;
					}


				}//for col ends here

				col_list = col_list->next;

			}//while ends

			x = 0;
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			fseek(fp, sizeof(table_file_header)+ (r)*(header->record_size) ,SEEK_SET);

			// printing last value...

			for (i=0; i< tab_entry->num_columns; i++ )

			{				

				//	printf("\n inside for: actual col name: %s",col_entry->col_name);
				//	printf("\n Comparing %s and %s the value is %d",col_list->col_name,col_entry->col_name,strcmpi( col_entry->col_name, col_list->col_name ));
				if (strcmpi( col_entry->col_name, col_list->col_name )== 0)
				{

					//		printf("\n inside if : actual col name: %s",col_entry->col_name);
					//		printf("\n inside if: actual col type: %d\n",col_entry->col_type);
					//read this value from file

					if(col_entry->col_type == T_CHAR)
					{

						char buffer[MAX_TOK_LEN];
						memset(buffer, 0, sizeof(buffer));
						fseek(fp,x,SEEK_CUR);

						char m;
						fread(&m, sizeof(char) , 1, fp);
						//fseek(fp,1,SEEK_CUR);

						if(m ==0)
						{
							fread(buffer,col_entry->col_len,1,fp);
							printf("-\t\t");
							col_entry++;
							break;

						}

						else
						{
							fread(buffer,col_entry->col_len,1,fp);
							printf("%s\t\t",buffer);
							col_entry++;
							break;

						}

					}

					else
					{	int *p;
					p = (int*)calloc(1,sizeof(int));						
					fseek(fp,x,SEEK_CUR);
					//fseek(fp,1,SEEK_CUR);

					char m;
					fread(&m,sizeof(char),1,fp);

					if (m ==0)
					{
						fread(p,sizeof(int),1,fp);
						printf("-\t\t");
						col_entry++;
						free(p);
						break;

					}

					else
					{
						fread(p,sizeof(int),1,fp);
						printf("%d\t\t",*p);
						col_entry++;
						free(p);
						break;


					}
					}

				}//if col is there 


				// else move to next col
				else

				{
					//printf("entering else if no match for last element\n");
					x += (col_entry->col_len +1 );						
					col_entry++;


				}


			}//for col ends here

			printf("\n");

		}//for rows ends here

		printf("%s size is :%d",filename,header->file_size);

		fflush(fp);
		fclose(fp);

	} //for file error


	return rc;
}


void add_to_colList(column_list **col_list ,char *value)
{

	//printf("entering add_to_ColumnList\n");
	//printf("%s\n",value);
	//printf("%s\n",*value);
	column_list *cur = *col_list;
	column_list *ptr = NULL;
	ptr= (column_list*)calloc(1,sizeof(column_list));

	strcpy(ptr->col_name,value);
	ptr->next = NULL;


	if( cur == NULL)
	{
		//printf("enter if\n");
		*col_list = ptr;

	}
	else
	{
		//  printf("entering else \n");
		while(cur ->next != NULL)
			cur=cur->next;	
		cur->next=ptr;

	}


	return;
}

void add_to_where_list(where_list **where_col_list ,char *col_name, int operator_sym ,char *value , int flag, int rel_op, int col_type)
{

	//printf("entering add_to_ColumnList\n");
	//printf("%s\n",value);
	//printf("%s\n",*value);
	where_list *cur = *where_col_list;
	where_list *ptr = NULL;
	ptr= (where_list*)calloc(1,sizeof(where_list));

	strcpy(ptr->col_name, col_name);
	ptr->arith_operator = operator_sym;
	strcpy(ptr->value , value);
	ptr->null_check = flag;
	ptr->relational_operator = rel_op;
	ptr->col_type = col_type;
	ptr->next = NULL;


	if( cur == NULL)
	{
		//printf("enter if\n");
		*where_col_list = ptr;

	}
	else
	{
		//  printf("entering else \n");
		while(cur ->next != NULL)
			cur=cur->next;	
		cur->next=ptr;

	}

	//return;
}



int sem_update_table(token_list *t_list)
{
	printf("enter update\n");

	int rc =0;
	tpd_entry  *tab_entry = NULL;
	token_list *cur;

	cur = t_list;

	where_list *where_col_list = NULL;
	where_col_list = (where_list*)calloc(1,sizeof(where_list) );


	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{

		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{

		//	printf("table name valid\n");

		tab_entry = get_tpd_from_list(cur->tok_string );

		//printf("after get tpd\n");
		if( tab_entry == NULL)
		{
			rc= TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
			printf("table does not exist in file\n");	
		}

		else
		{
			printf("table exists in file\n");
			char *tab= NULL;
			tab = (char*)calloc(MAX_IDENT_LEN, sizeof(char));
			strcpy(tab, tab_entry->table_name);
			cur=cur->next;

			if(cur->tok_value == K_SET)
			{
				cur=cur->next;
				if ((cur->tok_class != keyword) && (cur->tok_class != identifier) )
				{ 
					printf("XX\n");
					rc = INVALID_COLUMN_NAME;
					cur->tok_value= INVALID;

				}
				else
				{
					char *col_name = NULL;
					col_name =(char*)calloc(MAX_IDENT_LEN,sizeof(char));
					strcpy(col_name,cur->tok_string);

					//now check if this column name exists or not

					int a = get_col_from_table(col_name, tab);
					int b = get_colType_from_table(col_name , tab);

					if (a==0)
					{

						printf("Column found\n");
						cur= cur->next;

						if(cur->tok_value == S_EQUAL)
						{

							cur = cur->next;			

						if(cur->tok_class == constant)
						{
							if ( ( cur->tok_value == STRING_LITERAL && b !=T_CHAR  ) || ( cur->tok_value == INT_LITERAL && b != T_INT ))
						{
						  //error
							printf("DATA mis match during set update\n");
							rc = DATATYPE_MISMATCH_ERROR;
							cur->tok_value = INVALID;
							return rc;
						
						}
						
						
						else
						{
							char *value = NULL;
							value = (char*)calloc(MAX_IDENT_LEN, sizeof(char));
							strcpy(value,cur->tok_string);
							cur=cur->next;

							if(cur->tok_value == EOC)
							{
								printf("UPDATE Parsing successful\n");
								//call update function
								//	printf("calling teh update function \n");
								rc = update_file(tab ,col_name, value);

								if (rc == 0)
									printf("Update into file successful\n");

								else if( rc == -1)

									printf("Table Empty :No Rows Found to update\n");
								else
								{
									printf("Error: Update into file UNSUCCESSFUL\n");
									printf("Error: %d\n",rc);
								}

							}

							else if ( cur->tok_value== K_WHERE)
							{
								printf("Found WHERE clause\n" );
								cur= cur->next;

								if (cur->tok_class != identifier )
								{
									//error
									printf("not a valid identifier\n");
									rc = INVALID_UPDATE_STATEMENT;
									cur->tok_value= INVALID;
									//return rc;

								}
								else
								{
									printf("Found a valid column, now check if it exists in table\n");
									int  y = get_col_from_table(cur->tok_string,tab_entry->table_name);

									if (y != 0)
									{

										printf("column not found in table file\n");
										rc = COL_NOT_EXIST;
										cur->tok_value = INVALID;

									}

									else
									{
										int b = get_colType_from_table(cur->tok_string,tab_entry->table_name);
										char *column_name;
										column_name= (char*)calloc(1,MAX_IDENT_LEN);
										strcpy(column_name,cur->tok_string);

										printf("Found column\n");	

										cur = cur->next;

										if(cur->tok_value == S_EQUAL || cur->tok_value == S_GREATER|| cur->tok_value == S_LESS)
										{
											int arith_op = cur->tok_value;
											cur=cur->next;

											if(cur->tok_class == constant  )
											{

												if( ( cur->tok_value == STRING_LITERAL && b !=T_CHAR  ) || ( cur->tok_value == INT_LITERAL && b != T_INT ))
												{
													//error
													printf("DATA mis match during update with WHERE\n");
													rc = DATATYPE_MISMATCH_ERROR;
													cur->tok_value = INVALID;
													return rc;
																								
												}
												else
												{
													char *value1 = NULL;
													value1 = (char*)calloc(MAX_IDENT_LEN, sizeof(char));
													strcpy(value1,cur->tok_string);
													int col_type = cur->tok_value;
													int col_type1;

													if(col_type == STRING_LITERAL)
														col_type1 = T_CHAR;
													else if(col_type == INT_LITERAL)
														col_type1 = T_INT;


													cur = cur->next;

													if(cur->tok_value == EOC)
													{
														printf("UPDATE parsing with where clause Successful\n");
														add_to_where_list(&where_col_list,column_name, arith_op, value1, -1,EOC,col_type1);
														int rc = update_where_clause(where_col_list,tab,col_name, value);

														if(rc)
														printf("Error:%d\n",rc);

													}

													else
													{
														printf("update statement not ending correctly\n");
														rc = INVALID_UPDATE_STATEMENT;
														cur->tok_value= INVALID;

													}
												} //data mismatch
											} // constsnt
										
											else
											{
												printf("Constant not valid\n");
												rc = INVALID_UPDATE_STATEMENT;
												cur->tok_value = INVALID;
												return rc;


											}

										}
										else
										{
											printf("Invalid symbol \n");
											rc = INVALID_SYMBOL;
											cur->tok_value = INVALID;

											return rc;


										}
									}//valid col name

								} //valid identifier

							} //check where clause
							else
							{
								printf("update statement not ending correctly\n");
								rc = INVALID_UPDATE_STATEMENT;
								cur->tok_value= INVALID;
							}

						//	}
/*
							else
							{
							printf("not a valid string used to set\n");
							rc = INVALID_UPDATE_STATEMENT;
							cur->tok_value = INVALID;
							}
							
							*/

						 } // data mismatch

						 }
						else
						{
							printf("not a valid string used to set\n");
							rc = INVALID_UPDATE_STATEMENT;
							cur->tok_value = INVALID;
							}

						} // 

						else
						{
							printf("equal not present\n");
							rc = EQUAL_NOT_PRESENT;
							cur->tok_value= INVALID;


						}

					}

					else
					{
						printf("col not found\n");
						rc = COL_NOT_EXIST;
						cur->tok_value= INVALID;

					}

				}

			}

			else
			{
				printf("set not present\n");
				rc = INVALID_UPDATE_STATEMENT;
				cur->tok_value = INVALID;

			}

		}

	} //most outer else
	return rc;

}


int update_file(char *tab, char *col, char * value)
{

	int rc = 0;
	tpd_entry *tab_entry = get_tpd_from_list(tab);
	int x =0;
	cd_entry *col_entry;
	col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

	table_file_header *header;
	header = (table_file_header*)calloc(1, sizeof(table_file_header));



	char filename[MAX_IDENT_LEN];
	strcpy(filename,tab);
	strcat(filename , ".tab");


	FILE *fp= NULL;
	struct _stat file_stat;

	fp = fopen(filename ,"rbc");


	if(fp ==NULL)
	{
		rc = FILE_OPEN_ERROR;
	}

	else
	{
		_fstat(_fileno(fp), &file_stat);

		printf("Found the file\n");

		printf("Filesize =%d\n",file_stat.st_size);
		fread(header ,sizeof(table_file_header) ,1,fp);

	}

	printf("header record size : %d\n",header->record_size);
	printf("header record number : %d\n",header->num_records);

	fflush(fp);
	fclose(fp);

	printf( "col names :%s \n",col_entry->col_name);
	printf( "update value :%s \n",value);

	if(header->num_records == 0)
	{
		//printf("No Rows to update\n");
		rc = -1;
		return rc;
	
	}

	else
	{
	
		for (int i=1;i<= tab_entry->num_columns;i++)
		{
			if (strcmpi(col_entry->col_name, col)==0)
			{

				//  printf("col match\n");
				break;

			}

			else
			{

				//	printf("col didnt match: \n");
				//printf("old x: %d",x);

				x = x+ (col_entry->col_len +1);
				//printf("new x: %d",x);

				col_entry++;

			}

		} //for col ends here..


		if( (fp = fopen(filename,"r+b")) == NULL) 

		{
			rc = FILE_OPEN_ERROR;
		}

		else
		{

			fseek(fp,sizeof(table_file_header),SEEK_SET);
			fseek(fp,x ,SEEK_CUR);


			for (int j = 1 ; j<=header->num_records ; j++)
			{
				if(col_entry->col_type== T_CHAR)
				{
					//	printf("entreng TCHAR\n");

					if( col_entry->not_null == 1 )
					{
						//	printf("entreng TCHAR\n");
						if( strcmpi(value, "NULL") ==0)
							rc = INVALID_NOT_NULL_CONSTRAINT;

						else
						{

							char p = strlen(value);						
							fwrite(&p,sizeof(char),1,fp );

							char *buffer;
							buffer = (char*)calloc(MAX_IDENT_LEN,sizeof(char));
							buffer= value;
							fwrite(buffer,col_entry->col_len,1,fp);	

						}
					}

					else
					{
						//	printf("entering TCHAR for null\n");

						if( strcmpi(value, "NULL") == 0)
						{				
							char p = 0;
							fwrite(&p,sizeof(char),1,fp );

							char *buffer;
							buffer = (char*)calloc(MAX_IDENT_LEN,sizeof(char));
							buffer= value;
							fwrite(buffer,col_entry->col_len,1,fp);	

						}

						else
						{
							char p = strlen(value);
							//p = (int*)calloc(1,sizeof(char))
							fwrite(&p,sizeof(char),1,fp );

							char *buffer;
							buffer = (char*)calloc(MAX_IDENT_LEN,sizeof(char));
							buffer= value;
							fwrite(buffer,col_entry->col_len,1,fp);	

						}

					}

				} //T-_CHAR

				if(col_entry->col_type==T_INT)

				{

					//	printf("entreng TINT\n");

					if( col_entry->not_null ==1 )
					{
						if( strcmpi(value, "NULL")==0)
							rc = INVALID_NOT_NULL_CONSTRAINT;

						else
						{

							char c= 4;
							int num = atoi( value);
							fwrite(&c,1,1,fp);
							fwrite(&num,4,1,fp);

						}
					}

					else
					{
						//	printf("entreng TINT for null\n");

						if( strcmpi(value, "NULL")==0)
						{
							printf("check");
							char m = 0;
							fwrite(&m,1,1,fp);						
							int p = 0;
							fwrite(&p,sizeof(int),1,fp);
						}

						else
						{


							char c = 4;
							int num = atoi( value);
							fwrite(&c,1,1,fp);
							fwrite(&num,4,1,fp);

						}
					}


				} //T_int ends here

				int new_seek_length = (header->record_size - col_entry->col_len) - 1;
				fseek( fp, new_seek_length, SEEK_CUR);

			} // for rows end here

		} // file error
	} // if num_records 0

	fflush(fp);
	fclose(fp);

	return rc;
}


int sem_delete_table(token_list *tok_list)
{

	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry;

	where_list *where_col_list = NULL;
	where_col_list = (where_list*)calloc(MAX_NUM_COL,sizeof(where_list) );

	int flag = -1;


	cur = tok_list;

	if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&(cur->tok_class != type_name))
	{ 
		printf("Invalid Table Name\n");
		rc = INVALID_DELETE_STATEMENT;
		cur->tok_value= INVALID;

	}

	else
	{

		char *tab = NULL;
		tab = (char*)calloc(MAX_IDENT_LEN, sizeof(char));
		strcpy(tab,cur->tok_string);
		printf("%s\n",tab);

		tab_entry = get_tpd_from_list(tab);

		if (tab_entry == NULL)
		{
			printf("table does not exist in file\n");
			rc = TABLE_NOT_EXIST;
			cur->tok_value= INVALID;

		}

		else
		{
			printf("table exist in file\n");

			cur=cur->next;

			if (cur->tok_value == EOC  )
			{
				printf("Delete Parsing successful\n");
				int rc = delete_from_file(tab); //call to function

				if(rc == 0)
				{
					printf("Records deleted from table\n");			
				}

				else
					printf("FILE ERROR:Records could not be deleted from table\n");	

			}

			else if( cur->tok_value == K_WHERE)
			{
				cur = cur->next;

				printf("NN%s",cur->tok_string);
				if (cur->tok_class == identifier)
				{

					printf("Found a valid column, now check if it exists in table\n");
					int  y = get_col_from_table(cur->tok_string,tab);

					//printf("Y:%d\n",y);
					if (y != 0)
					{

						printf("column not found in table file\n");
						rc = COL_NOT_EXIST;
						cur->tok_value = INVALID;
						return rc;

					}

					else
					{
						char *column_name;
						column_name = (char*)calloc(1,MAX_IDENT_LEN);
						strcpy(column_name,cur->tok_string);

						printf("Found column\n");
						cur=cur->next;

						int col_type;
						col_type = get_colType_from_table(column_name,tab);

						if(cur->tok_value == S_EQUAL || cur->tok_value == S_GREATER|| cur->tok_value == S_LESS)
						{
							int arith_op = cur->tok_value;
							cur = cur->next;

							char *value1 = NULL;
							value1 = (char*)calloc(MAX_IDENT_LEN, sizeof(char));
							strcpy(value1,cur->tok_string);
							//printf("XXX:%s",cur->tok_string);

							if(cur->tok_class == constant)
							{
								if(cur->tok_value == STRING_LITERAL)
								{
									printf("STRING LITERAL\n");
									if(col_type == T_CHAR)
									{
										//if(arith_op == S_EQUAL)
										//{
											cur = cur->next;
											if(cur->tok_value == EOC)
											{
												printf("PARSING COMPLETE\n");
												add_to_where_list(&where_col_list,column_name, arith_op, value1, flag , EOC, col_type);
												int rc = delete_from_file_where_clause(where_col_list ,tab);
																						
											}
											else
											{
												//printf("EOC NUPUR\n");
												rc = INVALID_DELETE_STATEMENT;
												cur->tok_value =INVALID;
												return rc;
																						
											}
									//	} 

									/*	else
										{
											printf("Invalid Symbol \n");
											rc = INVALID_SYMBOL;
											cur->tok_value =INVALID;
											return rc;

										}
                                   */
									}
									else
									{
										rc = DATATYPE_MISMATCH_ERROR;
										cur->tok_value =INVALID;
										return rc;															
									}
																					
								}
								else if(cur->tok_value == INT_LITERAL)
								{
									printf("INTEGER LITERAL\n");
									if(col_type == T_INT)
									{
										cur = cur->next;
										if(cur->tok_value == EOC)
										{
											printf("PARSING COMPLETE\n");
											add_to_where_list(&where_col_list,column_name, arith_op, value1, flag , EOC, col_type);
											int rc = delete_from_file_where_clause(where_col_list ,tab);
										}
										else
										{
											rc = INVALID_DELETE_STATEMENT;
											cur->tok_value =INVALID;
											return rc;																						
										}
									}

									else
									{
										rc = DATATYPE_MISMATCH_ERROR;
										cur->tok_value =INVALID;
										return rc;
																
									}																						
								}
														
							}
							else
							{
								printf("Invalid CONSTSNT NUPUR\n");
								rc = INVALID_DELETE_STATEMENT;
								cur->tok_value = INVALID;
								return rc;
						
							}
						
						}

						else if( (cur->tok_value == K_IS ) && (cur->next->tok_value == K_NULL) )
						{
							printf("IS NULL CONDITION\n");
							flag = 1;
							cur = cur->next->next;
							if(cur->tok_value == EOC)
							{
								printf("PARSING COMPLETE\n");
								add_to_where_list(&where_col_list,column_name, 0, "", flag , EOC, col_type);
								int rc = delete_from_file_where_clause(where_col_list ,tab);
							}
							else
							{
								rc = INVALID_DELETE_STATEMENT;
								cur->tok_value =INVALID;
								return rc;																						
							}
					
						}

						else if ((cur->tok_value == K_IS ) && (cur->next->tok_value == K_NOT) && (cur->next->next->tok_value == K_NULL))
						{
							printf("IS NULL NOT CONDITION\n");
							flag = 0;
							cur = cur->next->next->next;
							if(cur->tok_value == EOC)
							{
								printf("PARSING COMPLETE\n");
								add_to_where_list(&where_col_list,column_name, 0, "", flag , EOC, col_type);
								int rc = delete_from_file_where_clause(where_col_list ,tab);
							}
							else
							{
								rc = INVALID_DELETE_STATEMENT;
								cur->tok_value =INVALID;
								return rc;																						
							}
						
						}

						else
						{
							printf("Not a valid symbol\n");
							rc = INVALID_SYMBOL;
							cur->tok_value = INVALID;
							return rc;											
						}									
					}	// valid col name					
				}

				else
				{
					printf("Invalid identifier NUPUR\n");
					rc = INVALID_DELETE_STATEMENT;
					cur->tok_value= INVALID;
					return rc;				
				}		
			} //where

			else
			{

				printf("Delete not defined properly\n");
				rc = INVALID_DELETE_STATEMENT;
				cur->tok_value= INVALID;

			}
		}

	}

	return rc;

}

int delete_from_file(char *tab)

{
	int rc = 0;

	char *filename = NULL;

	filename = (char*)calloc(MAX_TOK_LEN,sizeof(char));
	strcpy(filename,tab);
	strcat(filename, ".tab");
	table_file_header *header;


	FILE *fp;
	fp = fopen( filename ,"rbc");

	if(fp == NULL)
	{
		//error
		rc = FILE_OPEN_ERROR;
		printf("file error\n");


	}

	else
	{

		header = (table_file_header*)calloc(1,sizeof( table_file_header));

		fread(header, sizeof(table_file_header),1,fp);

		printf("Old size %d,%d,%d,%d,%d\n",header->file_size,header->record_size,header->num_records,header->record_offset,header->file_header_flag);

	}

	fflush(fp);
	fclose(fp);


	fp = fopen(filename,"wbc");
	if(fp == NULL)
	{	
		rc = FILE_OPEN_ERROR;
		printf("file error\n");

	}

	else
	{
		printf("Updating the header\n");
		header->num_records = 0;
		header->file_size = sizeof(table_file_header);

		fwrite(header, sizeof(table_file_header),1,fp);

	}
	printf("New size  %d,%d,%d,%d,%d\n",header->file_size,header->record_size,header->num_records,header->record_offset,header->file_header_flag);

	fflush(fp);
	fclose(fp);

	return rc;

}

int update_where_clause(where_list *where_col_list, char *tab, char * col_name,char* value2)
{
	printf("entering UPDATE for where clause\n");

	where_col_list = where_col_list->next;
	int rc =0;
	char filename[20] ;
	strcpy(filename , tab);
	strcat(filename, ".tab");


	int x = 0;
	int rowIndex[1000];
	int r = 0;

	//	printf("%s\n",filename);

	tpd_entry *tab_entry;
	tab_entry = get_tpd_from_list(tab);

	//first get the table and open that corresponding file

	FILE *fp;
	fp = fopen(filename, "rbc");
	int i = 1;

	struct _stat file_stat;
	_fstat(_fileno(fp), &file_stat);

	//printf("%s size is: %d\n",filename ,file_stat.st_size);


	if(fp == NULL)
	{

		printf("\nfile error\n");

		rc = FILE_OPEN_ERROR;

	}

	else
	{

		//first read the num of records from ur header

		table_file_header *header;
		header = (table_file_header*)calloc(1, sizeof(table_file_header));
		fread(header ,sizeof(table_file_header) ,1,fp);

		cd_entry *col_entry;	
		int numCol;
		int j;
		numCol = tab_entry->num_columns;

		fseek(fp,sizeof(table_file_header), SEEK_SET);
		col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

		//	printf("enter 1 for\n");

		printf("MAIN FUNCTION\n");

		//	while(where_col_list->next != NULL)
		//	{
		printf("entering while\n");
		//int x =0;
		//int rowIndex[1000];
		//int r = 0;
		//int rowIndex2[1000];
		//int q = 0;
		//int finalArr[1000];
		//int p = 0;
		int rel_op;

		printf("link list col name :%s\n",where_col_list->col_name);

		for(i = 1; i <= numCol ; i++ )

		{
			printf("entering for\n");
			if(strcmpi (where_col_list->col_name ,col_entry->col_name) == 0)

			{						
				printf("col found\n");
				fseek(fp , x, SEEK_CUR);

				//					if (where_col_list->null_check == -1)
				//					{
				if(where_col_list->col_type == col_entry->col_type)
				{
					if( where_col_list->col_type == T_CHAR )
					{
						char buffer[MAX_IDENT_LEN];
						memset(buffer ,0 ,sizeof(buffer)); 
						//fseek(fp,1,SEEK_CUR);
						char m;

						for(j=1; j <=header->num_records; j++)
						{
							printf("entering for row loop\n");			
							fread( &m,1,1,fp);
							fread(buffer,col_entry->col_len, 1, fp );	
							printf("%s\n",buffer);

							if(where_col_list->arith_operator== S_EQUAL) 
							{
								printf("Equal symbol\n");
								if (m !=0)
								{
									if(strcmp(buffer, where_col_list->value) == 0)
									{										
										printf("found data \n");
										rowIndex[r] = j;
										r++;

									}
								}
							}
							else if(where_col_list->arith_operator== S_LESS)
							{
								printf("Less symbol\n");

								if (m !=0)
								{
									if(strcmp(buffer, where_col_list->value) < 0)
									{										
										printf("found data \n");
										rowIndex[r] = j;
										r++;

									}
								}
															
							}

							else if(where_col_list->arith_operator== S_GREATER)
							{
								printf("More symbol\n");

								if (m !=0)
								{
									if(strcmp(buffer, where_col_list->value) > 0)
									{										
										printf("found data \n");
										rowIndex[r] = j;
										r++;

									}
								}
							}


							int new_seek_length = (header->record_size - col_entry->col_len) - 1;
							fseek( fp, new_seek_length, SEEK_CUR);

						} //for rows

						printf("r is:%d\n",r);
						printf("beofr free\n");

						printf("after free\n");
					}

					else if( where_col_list->col_type == T_INT)

					{

						printf("numeric data\n");
						int *p;
						p = (int*)calloc(1,sizeof(int));

						//fseek(fp,1,SEEK_CUR);
						char m;

						for(j=1; j <=header->num_records; j++)
						{
							printf("entering for row loop\n");	
							fread(&m,1,1,fp);
							fread(p, sizeof(int), 1, fp );	
							printf("%d\n",*p);

							if(where_col_list->arith_operator== S_EQUAL) 
							{
								printf("Equal symbol\n");
								if (m !=0)
								{

									if(*p == atoi(where_col_list->value) )
									{										
										printf("found data \n");
										rowIndex[r] = j;
										r++;

									}
								}
							}

							else if((where_col_list->arith_operator== S_LESS))
							{
								printf("Less symbol\n");
								if (m !=0)
								{

									if(*p < atoi(where_col_list->value) )
									{										
										printf("found data \n");
										rowIndex[r] = j;
										r++;

									}
								}

							}
							else if((where_col_list->arith_operator== S_GREATER))
							{
								printf("More symbol\n");
								if (m !=0)
								{

									if(*p > atoi(where_col_list->value) )
									{										
										printf("found data \n");
										rowIndex[r] = j;
										r++;

									}
								}

							}

							int new_seek_length = (header->record_size - sizeof(int)) - 1;
							fseek( fp, new_seek_length, SEEK_CUR);

						} //for rows

						printf("r is:%d\n",r);
						free(p);

					}
				} //data mismatch

				else								
				{							
					printf("data mismatch error\n");
					rc = DATATYPE_MISMATCH_ERROR;
					return rc;
					//break;

				}

				//				} //null check absent

				/*
				else
				{
				if(where_col_list->null_check ==1)
				{
				printf(" null condition\n");

				//fseek(fp,1,SEEK_CUR);
				char *m;
				m = (char*)calloc(1,sizeof(char));
				for(j=1; j <=header->num_records; j++)
				{
				printf("entering for row loop\n");								
				fread(m,1, 1, fp );									
				printf("%d\n",*m);

				if(*m == 0)
				{										
				printf("found data \n");
				rowIndex[r] = j;
				r++;

				}
				int new_seek_length = (header->record_size - 1);
				fseek( fp, new_seek_length, SEEK_CUR);

				} //for rows

				printf("r is:%d\n",r);
				free(m);



				}					
				else
				{
				printf(" NOT null condition\n");
				char *m;
				m = (char*)calloc(1,sizeof(char));

				for(j=1; j <=header->num_records; j++)
				{
				printf("entering for row loop\n");								
				fread(m,1, 1, fp );									
				printf("%d\n",*m);

				if(*m != 0)
				{										
				printf("found data \n");
				rowIndex[r] = j;
				r++;

				}
				int new_seek_length = (header->record_size - 1);
				fseek( fp, new_seek_length, SEEK_CUR);

				} //for rows

				printf("r is:%d\n",r);
				free(m);

				}
				} // null check present
				*/

				break;

			} //if col name found

			else
			{

				printf("column not found\n");
				x = x + col_entry->col_len+1;
				col_entry++;
			}

		}//for columns

		fclose(fp);
		fflush(fp);

	} //file read ends

	if( r == 0)
	{
		printf("*********************************************\n");
		printf("Warning :NO rows selected based on where condition\n");
		printf("*********************************************\n");
	}
	else
	{
		printf("********************************\n");
		printf("Updating %d Rows\n",r);
		printf("********************************\n");

		x = 0;
		cd_entry *col_entry;
		col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

		table_file_header *header;
		header = (table_file_header*)calloc(1, sizeof(table_file_header));


		//char filename[MAX_IDENT_LEN];
		//strcpy(filename,tab);
		//strcat(filename , ".tab");


		//FILE *fp= NULL;
		//struct _stat file_stat;

		fp = fopen(filename ,"rbc");


		if(fp ==NULL)
		{
			rc = FILE_OPEN_ERROR;
		}

		else
		{
			_fstat(_fileno(fp), &file_stat);

			printf("Found the file\n");

			printf("Filesize =%d\n",file_stat.st_size);
			fread(header ,sizeof(table_file_header) ,1,fp);

		}

		printf("header record size : %d\n",header->record_size);
		printf("header record number : %d\n",header->num_records);

		fflush(fp);
		fclose(fp);

		printf( "col names :%s \n",col_entry->col_name);
		printf( "update value :%s \n",value2);


		for (int i=1;i<= tab_entry->num_columns;i++)
		{
			if (strcmpi(col_entry->col_name, col_name)==0)
			{

				//printf("col match\n");
				break;

			}

			else
			{

				//	printf("col didnt match: \n");
				//printf("old x: %d",x);

				x = x + (col_entry->col_len +1);
				//printf("new x: %d",x);

				col_entry++;

			}

		} //for col ends here..


		if( (fp = fopen(filename,"r+b")) == NULL) 

		{
			rc = FILE_OPEN_ERROR;
		}

		else
		{

			//	fseek(fp,sizeof(table_file_header),SEEK_SET);
			//	fseek(fp,x ,SEEK_CUR);


			for (int j = 1; j <= r ;j++)
			{
				fseek(fp, (sizeof(table_file_header) + (header->record_size)*(rowIndex[j-1]-1)) ,SEEK_SET);
				fseek(fp, x ,SEEK_CUR);


				if(col_entry->col_type== T_CHAR)
				{
					//	printf("entreng TCHAR\n");

					if( col_entry->not_null == 1 )
					{
						//	printf("entreng TCHAR\n");
						if( strcmpi(value2, "NULL") == 0)
							rc = INVALID_NOT_NULL_CONSTRAINT;

						else
						{

							char p = strlen(value2);

							fwrite(&p,sizeof(char),1,fp );

							char *buffer;
							buffer = (char*)calloc(MAX_IDENT_LEN,sizeof(char));
							buffer= value2;
							fwrite(buffer,col_entry->col_len,1,fp);	

						}
					}

					else
					{
						//	printf("entering TCHAR for null\n");


						if( strcmpi(value2, "NULL") ==0)
						{
							char p = 0;
							fwrite(&p,sizeof(char),1,fp );

							char *buffer;
							buffer = (char*)calloc(MAX_IDENT_LEN,sizeof(char));
							buffer= value2;
							fwrite(buffer,col_entry->col_len,1,fp);	


						}

						else
						{
							char p = strlen(value2);
							//p = (int*)calloc(1,sizeof(char))
							fwrite(&p,sizeof(char),1,fp );

							char *buffer;
							buffer = (char*)calloc(MAX_IDENT_LEN,sizeof(char));
							buffer= value2;
							fwrite(buffer,col_entry->col_len,1,fp);	
						}

					}

				} //T-_CHAR

				if(col_entry->col_type==T_INT)

				{

					//	printf("entreng TINT\n");

					if( col_entry->not_null ==1 )
					{
						if( strcmpi(value2, "NULL")==0)
							rc = INVALID_NOT_NULL_CONSTRAINT;

						else
						{

							char c= 4;
							int num = atoi(value2);
							fwrite(&c,1,1,fp);
							fwrite(&num,4,1,fp);

						}
					}

					else
					{
						//	printf("entreng TINT for null\n");

						if( strcmpi(value2, "NULL")==0)
						{
							printf("check");
							char m = 0;
							fwrite(&m,1,1,fp);						
							int p = 0;
							fwrite(&p,sizeof(int),1,fp);
						}

						else
						{


							char c = 4;
							int num = atoi(value2);
							fwrite(&c,1,1,fp);
							fwrite(&num,4,1,fp);

						}
					}


				} //T_int ends here

				//	int new_seek_length = (header->record_size - col_entry->col_len) - 1;
				//	fseek( fp, new_seek_length, SEEK_CUR);

			} // for rows end here

			printf("********************************\n");
			printf("%d Rows successfully updated\n",r);
			printf("********************************\n");

		}

		fflush(fp);
		fclose(fp);

	}// rows returned
	return rc;
}

int select_col_where_clause(where_list *where_col_list, char *tab , column_list *col_list)
{
	printf("Entering select col where clause function\n");

	/*

	where_col_list = where_col_list->next;
	while(where_col_list->next != NULL)
	{
	printf("%s\n",where_col_list->col_name);
	printf("%d\n",where_col_list->null_check);
	printf("%d\n",where_col_list->arith_operator);
	printf("%s\n",where_col_list->value);
	printf("%d\n",where_col_list->col_type);		
	printf("%d\n",where_col_list->relational_operator);

	where_col_list = where_col_list->next;

	}

	printf("%s\n",where_col_list->col_name);
	printf("%d\n",where_col_list->null_check);
	printf("%d\n",where_col_list->arith_operator);
	printf("%s\n",where_col_list->value);
	printf("%d\n",where_col_list->col_type);		
	printf("%d\n",where_col_list->relational_operator);


	col_list = col_list->next;

	while(col_list->next != NULL)
	{
	printf("%s\t\t",col_list->col_name);
	col_list =col_list->next;
	}	
	printf("%s\t\t",col_list->col_name);

	printf("\n--------------------------------------------------------------------------\n");
	*/

	where_col_list = where_col_list->next;

	int rc =0;
	char filename[20] ;
	strcpy(filename , tab);
	strcat(filename, ".tab");
	//	printf("%s\n",filename);

	tpd_entry *tab_entry;
	tab_entry = get_tpd_from_list(tab);

	//first get the table and open that corresponding file

	FILE *fp;
	fp = fopen(filename, "rbc");
	int i = 1;

	struct _stat file_stat;
	_fstat(_fileno(fp), &file_stat);

	//printf("%s size is: %d\n",filename ,file_stat.st_size);


	if(fp == NULL)
	{

		printf("\nfile error\n");

		rc = FILE_OPEN_ERROR;

	}

	else
	{

		//first read the num of records from ur header

		table_file_header *header;
		header = (table_file_header*)calloc(1, sizeof(table_file_header));
		fread(header ,sizeof(table_file_header) ,1,fp);

		cd_entry *col_entry;	
		int numCol;
		int j;
		numCol = tab_entry->num_columns;

		fseek(fp,sizeof(table_file_header), SEEK_SET);
		col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

		//	printf("enter 1 for\n");

		printf("MAIN FUNCTION\n");

		//	while(where_col_list->next != NULL)
		//	{
		printf("entering while\n");
		int x =0;
		int rowIndex[1000];
		int r = 0;
		int rowIndex2[1000];
		int q = 0;
		int finalArr[1000];
		int p = 0;
		int rel_op;

		printf("link list col name :%s\n",where_col_list->col_name);

		for(i = 1; i <= numCol ; i++ )

		{
			printf("entering for\n");
			if(strcmpi (where_col_list->col_name ,col_entry->col_name) == 0)

			{						
				printf("col found\n");
				fseek(fp , x, SEEK_CUR);

				if (where_col_list->null_check == -1)
				{
					if(where_col_list->col_type == col_entry->col_type)
					{
						if( where_col_list->col_type == T_CHAR )
						{
							//char buffer[MAX_IDENT_LEN];
							char * buffer = (char *)calloc(1,MAX_TOK_LEN);
							//memset(buffer ,0 ,sizeof(buffer)); 
							//fseek(fp,1,SEEK_CUR);
							char m;

							for(j=1; j <=header->num_records; j++)
							{
								printf("entering for row loop\n");
								fread(&m,1,1,fp);
								fread(buffer,col_entry->col_len, 1, fp );	
								printf("%s\n",buffer);

								if(where_col_list->arith_operator== S_EQUAL) 
								{
									if (m !=0)
									{
										if(strcmp(buffer, where_col_list->value) == 0)
										{							

											printf("found data \n");
											rowIndex[r] = j;
											r++;

										}
									}
								}

								else if( where_col_list->arith_operator== S_LESS)
								{
									if (m != 0)
									{
										if(strcmp(buffer, where_col_list->value) < 0)
										{			

											printf("found data \n");
											rowIndex[r] = j;
											r++;

										}
									}
																
								}


								else if( where_col_list->arith_operator== S_GREATER)
								{
									if (m != 0)
									{
										if(strcmp(buffer, where_col_list->value) > 0)
										{							

											printf("found data \n");
											rowIndex[r] = j;
											r++;

										}
									}
																
								}
								
								int new_seek_length = (header->record_size - col_entry->col_len) - 1;
								fseek( fp, new_seek_length, SEEK_CUR);

							} //for rows

							printf("r is:%d\n",r);
							printf("beofr free\n");

							printf("after free\n");
							free(buffer);
						}

						else if( where_col_list->col_type == T_INT)

						{

							printf("numeric data\n");
							int *p;
							p = (int*)calloc(1,sizeof(int));

							char m;
							//fseek(fp,1,SEEK_CUR);

							for(j=1; j <=header->num_records; j++)
							{
								printf("entering for row loop\n");			
								fread(&m,1,1,fp);
								fread(p, sizeof(int), 1, fp );	
								printf("%d\n",*p);

								if(where_col_list->arith_operator== S_EQUAL) 
								{
									printf("Equal symbol\n");

									if( m !=0)
									{

										if(*p == atoi(where_col_list->value) )
										{										
											printf("found data \n");
											rowIndex[r] = j;
											r++;

										}
									}
								}

								else if((where_col_list->arith_operator== S_LESS))
								{
									printf("Less symbol\n");

									if( m !=0)
									{

										if(*p < atoi(where_col_list->value) )
										{										
											printf("found data \n");
											rowIndex[r] = j;
											r++;

										}

									}
								}
								else if((where_col_list->arith_operator== S_GREATER))
								{
									printf("More symbol\n");
									if( m !=0)
									{

										if(*p > atoi(where_col_list->value) )
										{										
											printf("found data \n");
											rowIndex[r] = j;
											r++;

										}
									}

								}

								int new_seek_length = (header->record_size - sizeof(int)) - 1;
								fseek( fp, new_seek_length, SEEK_CUR);

							} //for rows

							printf("r is:%d\n",r);
							free(p);

						}
					} //data mismatch

					else								
					{							
						printf("data mismatch error\n");
						rc = DATATYPE_MISMATCH_ERROR;							
						//break;
						return rc;

					}

				} //null check absent

				else
				{
					if(where_col_list->null_check ==1)
					{
						printf(" null condition\n");

						//fseek(fp,1,SEEK_CUR);
						char *m;
						m = (char*)calloc(1,sizeof(char));
						for(j=1; j <=header->num_records; j++)
						{
							printf("entering for row loop\n");								
							fread(m,1, 1, fp );									
							printf("%d\n",*m);

							if(*m == 0)
							{										
								printf("found data \n");
								rowIndex[r] = j;
								r++;

							}
							int new_seek_length = (header->record_size - 1);
							fseek( fp, new_seek_length, SEEK_CUR);

						} //for rows

						printf("r is:%d\n",r);
						free(m);



					}					
					else
					{
						printf(" NOT null condition\n");
						char *m;
						m = (char*)calloc(1,sizeof(char));

						for(j=1; j <=header->num_records; j++)
						{
							printf("entering for row loop\n");								
							fread(m,1, 1, fp );									
							printf("%d\n",*m);

							if(*m != 0)
							{										
								printf("found data \n");
								rowIndex[r] = j;
								r++;

							}
							int new_seek_length = (header->record_size - 1);
							fseek( fp, new_seek_length, SEEK_CUR);

						} //for rows

						printf("r is:%d\n",r);
						free(m);

					}
				} // null check present

				break;

			} //if col name found

			else
			{

				printf("column not found\n");
				x = x + col_entry->col_len+1;
				col_entry++;
			}

		}//for columns

		//		} // while

		if (where_col_list->relational_operator == EOC)
		{
			printf("only one where condition\n");

			for (int i = 0 ; i<r ; i++)
			{
				finalArr[p] = rowIndex[i];
				p++;
			}	

		}

		else
		{
			rel_op = where_col_list->relational_operator;

			printf("Two where condition\n");

			where_col_list=where_col_list->next;

			fseek(fp,sizeof(table_file_header), SEEK_SET);
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

			printf("entering while\n");
			x =0;
			//int rowIndex2[1000];
			//r = 0;

			printf("link list col name :%s\n",where_col_list->col_name);
			for(i = 1; i <= numCol ; i++ )

			{
				printf("entering for\n");
				if(strcmpi (where_col_list->col_name ,col_entry->col_name) == 0)

				{						
					printf("col found\n");
					fseek(fp , x, SEEK_CUR);

					if (where_col_list->null_check == -1)
					{
						if(where_col_list->col_type == col_entry->col_type)
						{
							if( where_col_list->col_type == T_CHAR )
							{
								//char buffer[MAX_IDENT_LEN];
								//memset(buffer ,0 ,sizeof(buffer)); 
								char *buffer = (char*)calloc(1,MAX_TOK_LEN);
								//fseek(fp,1,SEEK_CUR);
								char m;

								for(j=1; j <=header->num_records; j++)
								{
									printf("entering for row loop\n");		
									fread(&m , 1,1,fp);
									fread(buffer,col_entry->col_len, 1, fp );	
									printf("%s\n",buffer);

									if(where_col_list->arith_operator== S_EQUAL) 
									{
										printf("Equal symbol\n");

										if( m !=0)
										{
											if(strcmp(buffer, where_col_list->value) == 0)
											{										
												printf("found data \n");
												rowIndex2[q] = j;
												q++;

											}
										}
									}

									else if(where_col_list->arith_operator== S_LESS)
									{
										printf("< symbol\n");

										if( m !=0)
										{
											if(strcmp(buffer, where_col_list->value) < 0)
											{										
												printf("found data \n");
												rowIndex2[q] = j;
												q++;

											}
										}
									
									}

									else if(where_col_list->arith_operator== S_GREATER)
									{
										printf("> symbol\n");

										if( m !=0)
										{
											if(strcmp(buffer, where_col_list->value) > 0)
											{										
												printf("found data \n");
												rowIndex2[q] = j;
												q++;

											}
										}
									
									
									}

									int new_seek_length = (header->record_size - col_entry->col_len) - 1;
									fseek( fp, new_seek_length, SEEK_CUR);

								} //for rows

								printf("q is:%d\n",q);
								printf("beofr free\n");

								printf("after free\n");
								free(buffer);
							}

							else if( where_col_list->col_type == T_INT)

							{

								printf("numeric data\n");
								int *p;
								p = (int*)calloc(1,sizeof(int));

								char m;
								//fseek(fp,1,SEEK_CUR);

								for(j=1; j <=header->num_records; j++)
								{
									printf("entering for row loop\n");		
									fread(&m,1,1,fp);
									fread(p, sizeof(int), 1, fp );	
									printf("%d\n",*p);

									if(where_col_list->arith_operator== S_EQUAL) 
									{
										printf("Equal symbol\n");
										if( m !=0)
										{

											if(*p == atoi(where_col_list->value) )
											{										
												printf("found data \n");
												rowIndex2[q] = j;
												q++;

											}
										}
									}

									else if((where_col_list->arith_operator== S_LESS))
									{
										printf("Less symbol\n");
										if( m !=0)
										{

											if(*p < atoi(where_col_list->value) )
											{										
												printf("found data \n");
												rowIndex2[q] = j;
												q++;

											}
										}						
									}
									else if((where_col_list->arith_operator== S_GREATER))
									{
										printf("More symbol\n");
										if( m !=0)
										{

											if(*p > atoi(where_col_list->value) )
											{										
												printf("found data \n");
												rowIndex2[q] = j;
												q++;

											}

										}
									}

									int new_seek_length = (header->record_size - sizeof(int)) - 1;
									fseek( fp, new_seek_length, SEEK_CUR);

								} //for rows

								printf("q is:%d\n",q);
								free(p);

							}
						} //data mismatch

						else								
						{							
							printf("data mismatch error\n");
							rc = DATATYPE_MISMATCH_ERROR;
							//break;
							return rc;

						}

					} //null check absent

					else
					{
						if(where_col_list->null_check ==1)
						{
							printf(" null condition\n");

							//fseek(fp,1,SEEK_CUR);
							char *m;
							m = (char*)calloc(1,sizeof(char));
							for(j=1; j <=header->num_records; j++)
							{
								printf("entering for row loop\n");								
								fread(m,1, 1, fp );									
								printf("%d\n",*m);

								if(*m == 0)
								{										
									printf("found data \n");
									rowIndex2[q] = j;
									q++;

								}
								int new_seek_length = (header->record_size - 1);
								fseek( fp, new_seek_length, SEEK_CUR);

							} //for rows

							printf("q is:%d\n",q);
							free(m);



						}					
						else
						{
							printf(" NOT null condition\n");
							char *m;
							m = (char*)calloc(1,sizeof(char));

							for(j=1; j <=header->num_records; j++)
							{
								printf("entering for row loop\n");								
								fread(m,1, 1, fp );									
								printf("%d\n",*m);

								if(*m != 0)
								{										
									printf("found data \n");
									rowIndex2[q] = j;
									q++;

								}
								int new_seek_length = (header->record_size - 1);
								fseek( fp, new_seek_length, SEEK_CUR);

							} //for rows

							printf("q is:%d\n",q);
							free(m);

						}
					} // null check present

					break;

				} //if col name found

				else
				{

					printf("column not found\n");
					x = x + col_entry->col_len+1;
					col_entry++;
				}

			}//for columns

		} // 2nd where cond

		if(rel_op == K_OR)
		{
			printf("entering union\n");
			i = 0;
			j = 0;

			while(i < r && j < q)
			{

				if(rowIndex[i] < rowIndex2[j])
				{
					printf(" %d ", rowIndex[i]);
					finalArr[p] = rowIndex[i];
					i++;
					p++;
				}

				else if(rowIndex2[j] < rowIndex[i])
				{
					printf(" %d ", rowIndex2[j]);
					finalArr[p] = rowIndex2[j];
					j++;
					p++;

				}
				else
				{
					//printf("enter 3 loop\n");
					printf(" %d ", rowIndex2[j]);
					finalArr[p] = rowIndex2[j];
					//printf(" %d ", finalArr[p] );
					p++;
					j++;
					i++;
				}
			}

			/* Print remaining elements of the larger array */

			while(i < r)
			{
				printf(" %d ", rowIndex[i]);
				finalArr[p] = rowIndex[i];
				//printf(" %d ", finalArr[p] );
				i++;
				p++;

			}
			while(j < q)
			{

				printf(" %d ", rowIndex2[j]);
				finalArr[p] = rowIndex2[j];
				//printf(" %d ", finalArr[p] );
				j++;
				p++;
			}

			printf("after union\n");

			for(int u=0;u<p;u++)
			{	 
				printf("printing union array\n");
				printf("%d:\n",finalArr[u]);
			}	
		} // rel oper for OR


		if (rel_op == K_AND)
		{
			printf("entering intersection\n");
			int i = 0, j = 0;
			while ((i < r) && (j < q))
			{    
				if (rowIndex[i] < rowIndex2[j])
				{
					i++;
				}

				else if (rowIndex[i] > rowIndex2[j])
				{
					j++;
				}

				else
				{
					finalArr[p] = rowIndex[i];
					i++;
					j++;
					p++;
				}
			}		

			for(int u = 0 ; u < p ; u++)
			{	 
				printf("printing intersection array\n");
				printf("%d:\n",finalArr[u]);
			}


		} // for rel oper AND

		/*----------------------------------------MAIN LOGIC---------------------------------------------------------------------*/

		printf(" MAIN LOGIC \n");

		col_list = col_list->next;
		column_list *cl;

		cl = col_list;
		column_list *cl2;

		cl2 = col_list;

		while(cl->next != NULL)
		{
			printf("%s\t\t",cl->col_name);
			cl =cl->next;
		}	
		printf("%s\t\t",cl->col_name);

		printf("\n--------------------------------------------------------------------------\n");


		//now start reading these values from file

		printf("p is : %d\n",p);

		for(int r = 0 ; r < p ;r++)
		{
			fseek(fp, sizeof(table_file_header) + ((finalArr[r]-1)*(header->record_size)) ,SEEK_SET);
			col_list = cl2;
			int x = 0;
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

			while(col_list->next != NULL)
			{
				//	printf("entering while\n");	
				//	printf("\nname of the col paased in select:%s\n",col_list->col_name);

				x = 0;
				col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
				fseek(fp, sizeof(table_file_header)+ ((finalArr[r]-1)*(header->record_size)) ,SEEK_SET);


				for (i=0 ; i< tab_entry->num_columns; i++ )
				{
					//	printf("\n inside for: actual col name: %s",col_entry->col_name);
					//	printf("\n Comparing %s and %s the value is %d",col_list->col_name,col_entry->col_name,strcmpi( col_entry->col_name, col_list->col_name ));

					if (strcmpi( col_entry->col_name, col_list->col_name )== 0)
					{

						//printf("\n inside if : actual col name: %s",col_entry->col_name);
						//printf("\n inside if: actual col type: %d\n",col_entry->col_type);
						//read this value from file

						if(col_entry->col_type == T_CHAR)
						{							
							char buffer[MAX_TOK_LEN];
							memset(buffer,0,sizeof(buffer));						
							fseek(fp,x,SEEK_CUR);

							//fseek(fp,1,SEEK_CUR);

							char m;
							fread(&m,sizeof(char),1,fp);

							if (m == 0)
							{
								fread(buffer,sizeof(char)*col_entry->col_len,1,fp);
								printf("-\t\t");
								col_entry++;
								break;

							}

							else
							{
								fread(buffer,sizeof(char)*col_entry->col_len,1,fp);
								printf("%s\t\t",buffer);
								col_entry++;
								break;

							}

						}

						else
						{	int *p;
						p = (int*)calloc(1,sizeof(int));
						fseek(fp,x,SEEK_CUR);
						//	fseek(fp,1,SEEK_CUR);

						char m;
						fread(&m,1,1,fp);

						if(m == 0)
						{
							fread(p,sizeof(int),1,fp);
							printf("-\t\t");
							col_entry++;
							free(p);
							break;

						}

						else
						{
							fread(p,sizeof(int),1,fp);
							printf("%d\t\t",*p);
							col_entry++;
							free(p);
							break;

						}


						}

					}//if col is there 


					// else move to next col
					else	
					{
						//printf("entering else if no match\n");				
						x += (col_entry->col_len +1 );
						col_entry++;
					}


				}//for col ends here

				col_list = col_list->next;

			}//while ends

			x = 0;
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			fseek(fp, sizeof(table_file_header)+ (finalArr[r]-1)*(header->record_size) ,SEEK_SET);

			// printing last value...

			for (i=0; i< tab_entry->num_columns; i++ )

			{				

				//	printf("\n inside for: actual col name: %s",col_entry->col_name);
				//	printf("\n Comparing %s and %s the value is %d",col_list->col_name,col_entry->col_name,strcmpi( col_entry->col_name, col_list->col_name ));
				if (strcmpi( col_entry->col_name, col_list->col_name )== 0)
				{

					//		printf("\n inside if : actual col name: %s",col_entry->col_name);
					//		printf("\n inside if: actual col type: %d\n",col_entry->col_type);
					//read this value from file

					if(col_entry->col_type == T_CHAR)
					{

						char buffer[MAX_TOK_LEN];
						memset(buffer, 0, sizeof(buffer));
						fseek(fp,x,SEEK_CUR);

						char m;
						fread(&m, sizeof(char) , 1, fp);
						//fseek(fp,1,SEEK_CUR);

						if(m ==0)
						{
							fread(buffer,col_entry->col_len,1,fp);
							printf("-\t\t");
							col_entry++;
							break;

						}

						else
						{
							fread(buffer,col_entry->col_len,1,fp);
							printf("%s\t\t",buffer);
							col_entry++;
							break;

						}

					}

					else
					{	int *p;
					p = (int*)calloc(1,sizeof(int));						
					fseek(fp,x,SEEK_CUR);
					//fseek(fp,1,SEEK_CUR);

					char m;
					fread(&m,sizeof(char),1,fp);

					if (m ==0)
					{
						fread(p,sizeof(int),1,fp);
						printf("-\t\t");
						col_entry++;
						free(p);
						break;

					}

					else
					{
						fread(p,sizeof(int),1,fp);
						printf("%d\t\t",*p);
						col_entry++;
						free(p);
						break;


					}
					}

				}//if col is there 


				// else move to next col
				else

				{
					//printf("entering else if no match for last element\n");
					x += (col_entry->col_len +1 );						
					col_entry++;


				}


			}//for col ends here

			printf("\n");

		}//for rows ends here

		printf("%s size is :%d\n",filename,header->file_size);

		fflush(fp);
		fclose(fp);


	}//file error

	return rc;
}

int select_aggregate(where_list *where_col_list, char *tab ,int star, int aggregate,char* col_name)
{

	printf("Entering select AGGREGATE\n");

	int rc =0;
	char filename[20] ;
	strcpy(filename , tab);
	strcat(filename, ".tab");

	tpd_entry *tab_entry;
	tab_entry = get_tpd_from_list(tab);

	cd_entry *col_entry;	

	int x =0;
	int rowIndex[1000];
	int r = 0;
	int rowIndex2[1000];
	int q = 0;
	int finalArr[1000];
	int p = 0;
	int rel_op;


	FILE *fp;
	int i = 1;

	struct _stat file_stat;

	fp = fopen(filename, "rbc");
	_fstat(_fileno(fp), &file_stat);

	table_file_header *header;
	header = (table_file_header*)calloc(1, sizeof(table_file_header));

	/*
	int rc =0;
	char filename[20] ;
	strcpy(filename , tab);
	strcat(filename, ".tab");


	tpd_entry *tab_entry;
	tab_entry = get_tpd_from_list(tab);

	*/
	//first get the table and open that corresponding file

	/*
	FILE *fp;
	fp = fopen(filename, "rbc");
	int i = 1;

	struct _stat file_stat;
	_fstat(_fileno(fp), &file_stat);

	//printf("%s size is: %d\n",filename ,file_stat.st_size);

	*/
	if(fp == NULL)
	{

		printf("\nfile error\n");

		rc = FILE_OPEN_ERROR;

	}

	else
	{
		if( where_col_list != NULL)
		{
			printf("Where clause for Aggregate\n");	
			where_col_list = where_col_list->next;

			//first read the num of records from ur header

			//	table_file_header *header;
			//	header = (table_file_header*)calloc(1, sizeof(table_file_header));
			fread(header ,sizeof(table_file_header) ,1,fp);

			cd_entry *col_entry;	
			int numCol;
			int j;
			numCol = tab_entry->num_columns;

			fseek(fp,sizeof(table_file_header), SEEK_SET);
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

			printf("MAIN FUNCTION\n");
			/*
			printf("entering while\n");
			int x =0;
			int rowIndex[1000];
			int r = 0;
			int rowIndex2[1000];
			int q = 0;
			int finalArr[1000];
			int p = 0;
			int rel_op;
			*/
			printf("link list col name :%s\n",where_col_list->col_name);

			for(i = 1; i <= numCol ; i++ )

			{
				printf("entering for\n");
				if(strcmpi (where_col_list->col_name ,col_entry->col_name) == 0)

				{						
					//printf("col found\n");
					fseek(fp , x, SEEK_CUR);

					if (where_col_list->null_check == -1)
					{
						if(where_col_list->col_type == col_entry->col_type)
						{
							if( where_col_list->col_type == T_CHAR )
							{
								//char buffer[MAX_IDENT_LEN];
								//memset(buffer ,0 ,sizeof(buffer)); 
								char *buffer = (char*)calloc(1,MAX_IDENT_LEN);
								//fseek(fp,1,SEEK_CUR);
								char m;

								for(j=1; j <=header->num_records; j++)
								{
									//printf("entering for row loop\n");		
									fread(&m,1,1,fp);
									fread(buffer,col_entry->col_len, 1, fp );	
									//printf("%s\n",buffer);

									if (where_col_list->arith_operator== S_EQUAL)
									{
										if( m !=0)
										{

											if(strcmp(buffer, where_col_list->value) == 0)
											{										
												//printf("found data \n");
												rowIndex[r] = j;
												r++;

											}
										}
									}

									else if(where_col_list->arith_operator== S_LESS)
									{
										if( m !=0)
										{

											if(strcmp(buffer, where_col_list->value) < 0)
											{										
												//printf("found data \n");
												rowIndex[r] = j;
												r++;

											}
										}								
									
									}


									else if(where_col_list->arith_operator== S_GREATER)
									{
										if( m !=0)
										{

											if(strcmp(buffer, where_col_list->value) > 0)
											{										
												//printf("found data \n");
												rowIndex[r] = j;
												r++;

											}
										}									
									
									}
									int new_seek_length = (header->record_size - col_entry->col_len) - 1;
									fseek( fp, new_seek_length, SEEK_CUR);

								} //for rows

								printf("r is:%d\n",r);
								//printf("beofr free\n");

								//printf("after free\n");
							}

							else if( where_col_list->col_type == T_INT)

							{

								//printf("numeric data\n");
								int *p;
								p = (int*)calloc(1,sizeof(int));

								char m;
								//fseek(fp,1,SEEK_CUR);

								for(j=1; j <=header->num_records; j++)
								{
									//printf("entering for row loop\n");		
									fread(&m,1,1,fp);
									fread(p, sizeof(int), 1, fp );	
									//printf("%d\n",*p);

									if(where_col_list->arith_operator== S_EQUAL) 
									{
										//printf("Equal symbol\n");
										if(m !=0)
										{

											if(*p == atoi(where_col_list->value) )
											{										
												//printf("found data \n");
												rowIndex[r] = j;
												r++;

											}
										}
									}

									else if((where_col_list->arith_operator== S_LESS))
									{
										//printf("Less symbol\n");
										if (m!=0)
										{

											if(*p < atoi(where_col_list->value) )
											{										
												//printf("found data \n");
												rowIndex[r] = j;
												r++;

											}
										}							
									}
									else if((where_col_list->arith_operator== S_GREATER))
									{
										//printf("More symbol\n");
										if(m !=0)
										{

											if(*p > atoi(where_col_list->value) )
											{										
												//printf("found data \n");
												rowIndex[r] = j;
												r++;

											}
										}

									}

									int new_seek_length = (header->record_size - sizeof(int)) - 1;
									fseek( fp, new_seek_length, SEEK_CUR);

								} //for rows

								printf("r is:%d\n",r);
								free(p);

							}
						} //data mismatch

						else								
						{							
							printf("data mismatch error\n");
							rc = DATATYPE_MISMATCH_ERROR;
							//break;
							return rc;

						}

					} //null check absent

					else
					{
						if(where_col_list->null_check ==1)
						{
							//printf(" null condition\n");

							//fseek(fp,1,SEEK_CUR);
							char *m;
							m = (char*)calloc(1,sizeof(char));
							for(j=1; j <=header->num_records; j++)
							{
								//printf("entering for row loop\n");								
								fread(m,1, 1, fp );									
								//printf("%d\n",*m);

								if(*m == 0)
								{										
									//printf("found data \n");
									rowIndex[r] = j;
									r++;

								}
								int new_seek_length = (header->record_size - 1);
								fseek( fp, new_seek_length, SEEK_CUR);

							} //for rows

							printf("r is:%d\n",r);
							free(m);



						}					
						else
						{
							//printf(" NOT null condition\n");
							char *m;
							m = (char*)calloc(1,sizeof(char));

							for(j=1; j <=header->num_records; j++)
							{
								printf("entering for row loop\n");								
								fread(m,1, 1, fp );									
								//printf("%d\n",*m);

								if(*m != 0)
								{										
									//printf("found data \n");
									rowIndex[r] = j;
									r++;

								}
								int new_seek_length = (header->record_size - 1);
								fseek( fp, new_seek_length, SEEK_CUR);

							} //for rows

							printf("r is:%d\n",r);
							free(m);

						}
					} // null check present

					break;

				} //if col name found

				else
				{

					printf("column not found\n");
					x = x + col_entry->col_len+1;
					col_entry++;
				}

			}//for columns

			//		} // while

			if (where_col_list->relational_operator == EOC)
			{
				printf("only one where condition\n");

				for (int i = 0 ; i<r ; i++)
				{
					finalArr[p] = rowIndex[i];
					p++;
				}	

			}

			else
			{
				rel_op = where_col_list->relational_operator;

				printf("Two where condition\n");

				where_col_list=where_col_list->next;

				fseek(fp,sizeof(table_file_header), SEEK_SET);
				col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

				//printf("entering while\n");
				x =0;
				//int rowIndex2[1000];
				//r = 0;

				printf("link list col name :%s\n",where_col_list->col_name);
				for(i = 1; i <= numCol ; i++ )

				{
					//printf("entering for\n");
					if(strcmpi (where_col_list->col_name ,col_entry->col_name) == 0)

					{						
						//printf("col found\n");
						fseek(fp , x, SEEK_CUR);

						if (where_col_list->null_check == -1)
						{
							if(where_col_list->col_type == col_entry->col_type)
							{
								if( where_col_list->col_type == T_CHAR )
								{
									//char buffer[MAX_IDENT_LEN];
									//memset(buffer ,0 ,sizeof(buffer)); 
									char *buffer = (char*)calloc(1,MAX_IDENT_LEN);
									//fseek(fp,1,SEEK_CUR);
									char m;
									for(j=1; j <=header->num_records; j++)
									{
										//printf("entering for row loop\n");		
										fread(&m,1,1,fp);
										fread(buffer,col_entry->col_len, 1, fp );	
										//printf("%s\n",buffer);

										if(where_col_list->arith_operator== S_EQUAL) 
										{
											//printf("Equal symbol\n");

											if(m !=0)
											{
												if(strcmp(buffer, where_col_list->value) == 0)
												{										
													//printf("found data \n");
													rowIndex2[q] = j;
													q++;

												}
											}

										}
										else if(where_col_list->arith_operator== S_LESS) 
										{
											//printf("Less symbol\n");

											if(m !=0)
											{
												if(strcmp(buffer, where_col_list->value) < 0)
												{										
													//printf("found data \n");
													rowIndex2[q] = j;
													q++;

												}
											}

										}

										else if(where_col_list->arith_operator== S_GREATER) 
										{
											//printf("More symbol\n");

											if(m !=0)
											{
												if(strcmp(buffer, where_col_list->value) > 0)
												{										
													//printf("found data \n");
													rowIndex2[q] = j;
													q++;

												}
											}

										}

										int new_seek_length = (header->record_size - col_entry->col_len) - 1;
										fseek( fp, new_seek_length, SEEK_CUR);

									} //for rows

									printf("q is:%d\n",q);
								//	printf("beofr free\n");

									//printf("after free\n");
								}

								else if( where_col_list->col_type == T_INT)

								{

									//printf("numeric data\n");
									int *p;
									p = (int*)calloc(1,sizeof(int));

									//fseek(fp,1,SEEK_CUR);
									char m;

									for(j=1; j <=header->num_records; j++)
									{
										//printf("entering for row loop\n");
										fread(&m,1,1,fp);
										fread(p, sizeof(int), 1, fp );	
										//printf("%d\n",*p);

										if(where_col_list->arith_operator== S_EQUAL) 
										{
											//printf("Equal symbol\n");
											if(m !=0)
											{

												if(*p == atoi(where_col_list->value) )
												{										
													//printf("found data \n");
													rowIndex2[q] = j;
													q++;

												}
											}
										}

										else if((where_col_list->arith_operator== S_LESS))
										{
											//printf("Less symbol\n");
											if (m !=0)
											{

												if(*p < atoi(where_col_list->value) )
												{										
													//printf("found data \n");
													rowIndex2[q] = j;
													q++;

												}
											}

										}
										else if((where_col_list->arith_operator== S_GREATER))
										{
											//printf("More symbol\n");
											if (m !=0)
											{

												if(*p > atoi(where_col_list->value) )
												{										
													//printf("found data \n");
													rowIndex2[q] = j;
													q++;

												}

											}
										}

										int new_seek_length = (header->record_size - sizeof(int)) - 1;
										fseek( fp, new_seek_length, SEEK_CUR);

									} //for rows

									printf("q is:%d\n",q);
									free(p);

								}
							} //data mismatch

							else								
							{							
								printf("data mismatch error\n");
								rc = DATATYPE_MISMATCH_ERROR;
								//break;
								return rc;

							}

						} //null check absent

						else
						{
							if(where_col_list->null_check ==1)
							{
								//printf(" null condition\n");

								//fseek(fp,1,SEEK_CUR);
								char *m;
								m = (char*)calloc(1,sizeof(char));
								for(j=1; j <=header->num_records; j++)
								{
									//printf("entering for row loop\n");								
									fread(m,1, 1, fp );									
									//printf("%d\n",*m);

									if(*m == 0)
									{										
										//printf("found data \n");
										rowIndex2[q] = j;
										q++;

									}
									int new_seek_length = (header->record_size - 1);
									fseek( fp, new_seek_length, SEEK_CUR);

								} //for rows

								printf("q is:%d\n",q);
								free(m);



							}					
							else
							{
								//printf(" NOT null condition\n");
								char *m;
								m = (char*)calloc(1,sizeof(char));

								for(j=1; j <=header->num_records; j++)
								{
									//printf("entering for row loop\n");								
									fread(m,1, 1, fp );									
									//printf("%d\n",*m);

									if(*m != 0)
									{										
										//printf("found data \n");
										rowIndex2[q] = j;
										q++;

									}
									int new_seek_length = (header->record_size - 1);
									fseek( fp, new_seek_length, SEEK_CUR);

								} //for rows

								printf("q is:%d\n",q);
								free(m);

							}
						} // null check present

						break;

					} //if col name found

					else
					{

						//printf("column not found\n");
						x = x + col_entry->col_len+1;
						col_entry++;
					}

				}//for columns

			} // 2nd where cond

			if(rel_op == K_OR)
			{
				//printf("before union\n");
				i = 0;
				j = 0;

				while(i < r && j < q)
				{

					if(rowIndex[i] < rowIndex2[j])
					{
						//printf(" %d ", rowIndex[i]);
						finalArr[p] = rowIndex[i];
						i++;
						p++;
					}

					else if(rowIndex2[j] < rowIndex[i])
					{
						//printf(" %d ", rowIndex2[j]);
						finalArr[p] = rowIndex2[j];
						j++;
						p++;

					}
					else
					{
						//printf("enter 3 loop\n");
						//printf(" %d ", rowIndex2[j]);
						finalArr[p] = rowIndex2[j];
						//printf(" %d ", finalArr[p] );
						p++;
						j++;
						i++;
					}
				}

				/* Print remaining elements of the larger array */

				while(i < r)
				{
					//printf(" %d ", rowIndex[i]);
					finalArr[p] = rowIndex[i];
					//printf(" %d ", finalArr[p] );
					i++;
					p++;

				}
				while(j < q)
				{

					//printf(" %d ", rowIndex2[j]);
					finalArr[p] = rowIndex2[j];
					//printf(" %d ", finalArr[p] );
					j++;
					p++;
				}

				//printf("after union\n");

				for(int u=0;u<p;u++)
				{	 
					//printf("printing union array\n");
					//printf("%d:",finalArr[u]);
				}	
			} // rel oper for OR


			if (rel_op == K_AND)
			{
				//printf("enter intersection array\n");
				int i = 0, j = 0;
				while ((i < r) && (j < q))
				{    
					if (rowIndex[i] < rowIndex2[j])
					{
						i++;
					}

					else if (rowIndex[i] > rowIndex2[j])
					{
						j++;
					}

					else
					{
						finalArr[p] = rowIndex[i];
						i++;
						j++;
						p++;
					}
				}		

				for(int u=0;u<p;u++)
				{	 
					//printf("printing intersection array\n");
					//printf("%d:",finalArr[u]);
				}


			} // for rel oper AND

		} // where_list present

		else
		{
			//printf("hii\n");
			fread(header ,sizeof(table_file_header) ,1,fp);
			p = header->num_records;
			//printf("p is:%d\n",p);

			for( int i=0; i<p; i++)
			{
				finalArr[i] = i+1 ;

			}
		}

		printf("final array has %d elements\n",p);


		/*--------------------------------MAIN LOGIC-----------------------------------*/

		int counter = 0;
		int sum = 0;

		for(int r = 0 ; r < p ;r++)
		{

			x = 0;
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			fseek(fp, sizeof(table_file_header) + ((finalArr[r]-1)*(header->record_size)) ,SEEK_SET);


			for (i=0 ; i< tab_entry->num_columns; i++ )
			{

				if (strcmpi( col_entry->col_name,col_name )== 0)
				{

					if(col_entry->col_type == T_CHAR)
					{							
						char buffer[MAX_TOK_LEN];
						memset(buffer,0,sizeof(buffer));						
						fseek(fp,x,SEEK_CUR);

						//fseek(fp,1,SEEK_CUR);

						char m;
						fread(&m,sizeof(char),1,fp);

						if (m == 0)
						{
							fread(buffer,sizeof(char)*col_entry->col_len,1,fp);
							//printf("%s\t\t",buffer);
							//printf("-\t\t");
							counter = counter+1;
							//printf("counter:%d\n" ,counter);
							col_entry++;
							break;

						}

						else
						{
							fread(buffer,sizeof(char)*col_entry->col_len,1,fp);
							//printf("%s\t\t",buffer);
							col_entry++;
							break;

						}

					}

					else
					{	int *p;
					p = (int*)calloc(1,sizeof(int));
					fseek(fp,x,SEEK_CUR);
					//	fseek(fp,1,SEEK_CUR);

					char m;
					fread(&m,1,1,fp);

					if(m == 0)
					{
						//fread(p,sizeof(int),1,fp);
						//printf("-\t\t");
						counter = counter +1;
						sum = sum + 0;
						col_entry++;
						free(p);
						break;

					}

					else
					{
						fread(p,sizeof(int),1,fp);
						//printf("%d\t\t",*p);
						sum = sum + *p;
						col_entry++;
						free(p);
						break;

					}


					}

				}//if col is there 


				// else move to next col
				else	
				{
					//printf("entering else if no match\n");				
					x += (col_entry->col_len +1 );
					col_entry++;
				}


			}//for col ends here

			//	printf("\n");

		}//for rows ends here

		if (p != 0)
		{
			if (aggregate== F_COUNT)
			{
				if (star == 0)
				{
					//printf("Total values :%d\n",p);
					//printf("Null values :%d\n",counter);
					printf("\n********************\n");
					printf("COUNT(%s) is %d\n",col_name, p - counter);
					printf("********************\n");
				}
				else
				{
					printf("\n********************\n");
					printf("COUNT(*) is %d\n",p);
					printf("********************\n");

				}

			}

			else if(aggregate== F_SUM)
			{
				printf("\n********************\n");
				printf("SUM(%s) is %d\n",col_name,sum);
				printf("********************\n");

			}

			else if(aggregate==F_AVG)
			{

				//printf("Null values of %s :%d\n",col_name,counter);
				//printf("non-null values of %s is %d\n",col_name, p - counter);
				printf("\n********************\n");
				printf("AVG(%s) is %d\n",col_name,sum/(p-counter));
				printf("********************\n");
			}

		}

		else
		{
			printf("NO rows in final Array\n");



		}
		printf("\n%s size is :%d\n",filename,header->file_size);

		fflush(fp);
		fclose(fp);


	}// file -error


	return rc;

}

int select_order_by(where_list *where_col_list,column_list *col_list, char *tab, char *order_by_col_name,int col_type,int desc)
{
	//printf("Enter the order by\n");
	int rc = 0;
	int size = 0;
	int *res_array = 0;

	
	char filename[20] ;
	strcpy(filename , tab);
	strcat(filename, ".tab");

	FILE *fp;

	table_file_header *header;
	header = (table_file_header*)calloc(1, sizeof(table_file_header));

	if( (fp = fopen(filename,"rbc")) == NULL) 
	{
		rc = FILE_OPEN_ERROR;
	}

	else
	{

	fread(header ,sizeof(table_file_header) ,1,fp);
	fclose(fp);
	fflush(fp);

	}
	
	if (where_col_list == NULL)
	{
		int total =1;
		//printf("nupur CREATE111111111\n");

		int all_row_array[1000];
		
		for (total = 1; total <= header->num_records; total++)
		{
			//printf("FOR\n");			
			all_row_array[total] = total;
			printf("value:%d",all_row_array[total]);
		}

		printf("Total is %d",total-1);

		char stringArray[50][MAX_IDENT_LEN];
		int num_rows[50];
		int null_rows[50];
		int numArray[2][1000];
		int nullArray[2][1000];
		int n = 0;
		int h = 0;
		int finalArray[1000];

		tpd_entry *tab_entry;
		tab_entry = get_tpd_from_list(tab);

		int x = 0;
		int j;
		int numCol = tab_entry->num_columns;

		cd_entry *col_entry;
		col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);


		for (int i = 1; i<= tab_entry->num_columns;i++)
		{
			if (strcmpi(col_entry->col_name, order_by_col_name) == 0)
			{
				//printf("col match\n");
				break;		
			}
			else
			{

				x = x + (col_entry->col_len +1);
				col_entry++;

			}

		} //for col ends here..

		if( (fp = fopen(filename,"rbc")) == NULL) 
		{
			rc = FILE_OPEN_ERROR;
		}

	else
	{
		table_file_header *header;
		header = (table_file_header*)calloc(1, sizeof(table_file_header));
		fread(header ,sizeof(table_file_header) ,1,fp);
						

		for (int j = 1; j <= total -1 ;j++)
		{
			fseek(fp, (sizeof(table_file_header) + (header->record_size)*(all_row_array[j]-1)), SEEK_SET);
			fseek(fp, x ,SEEK_CUR);

			if(col_type== T_CHAR)
			{
				//printf("entreng TCHAR\n");
				char m;
				fread(&m,1,1,fp);
				char buffer[MAX_IDENT_LEN];
				//int p;
				fread(buffer, col_entry->col_len ,1, fp);

				if ( m != 0)
				{
					//printf("XXXX!!");
					//printf("p is:%d\n",p);
					//numArray[0][n] = p;
					strcpy(stringArray[n] ,buffer);
					num_rows[n] = all_row_array[j];
					n++;
					printf("n :%d",n);

				}
				else
				{
					null_rows[h] = all_row_array[j];
					h++;					
				}

			} //T-_CHAR

			if(col_type == T_INT)
			{
				//printf("entreng TINT\n");
				char m;
				fread(&m,1,1,fp);
				int p;
				fread(&p,sizeof(int),1,fp);
				//printf(

				if ( m != 0)
				{
					//printf("p is:%d\n",p);
					numArray[0][n] = p;
					//numArray[n][1]= rowIndex[j-1];
					//printf("row index nupur:%d\n",*(res_array + j-1));
					numArray[1][n] = all_row_array[j];
					//numArray[1][n] = j;
					n++;
				}

				else
				{
					//nullArray[h][1]=rowIndex[j-1];
					//nullArray[h][0] = -1;
					nullArray[1][h] = all_row_array[j];
					//nullArray[1][h] = j;
					h++;

				}

			} //T_int ends here

		} // for rows end here

		
		fflush(fp);
		fclose(fp);

	} // file error


    if(col_type == T_CHAR)
	{
		int temp1;

		//printf("L\n");		

		//printf("N is %d",n);
		//printf("H is %d",h);

		for(int i = 0 ;i<n ;i++)
		{
			//printf("N");		
			//printf("%s",stringArray[i]);
		}

		if (desc == 0)
		{
			for(int i = 0; i< n ;i++)
			{
				for(int j = i+1; j< n ;j++)
				{
					if (strcmp(stringArray[i],stringArray[j]) > 0)
					{
						//printf("DD\n");
						temp1 = num_rows[i];
						num_rows[i] = num_rows[j];
						num_rows[j] = temp1;
						
					}
		
				}
		
			}

		}

		if (desc == 1)
		{
			for(int i = 0; i< n ;i++)
			{
				for(int j =i+1; j< n ;j++)
				{

					if (strcmp(stringArray[i],stringArray[j]) < 0)
					{

						temp1 = num_rows[i];
						num_rows[i] = num_rows[j];
						num_rows[j] = temp1;
			
					}		
				}		
			}
		}
		//////////////////////////
		int f = 1;
		if (desc == 1)
		{
			for( int i = 0; i< h; i++)
			{
				finalArray[f] = null_rows[i];
				f++;				
			}

			for (int i=0; i< n ;i++)
			{
				finalArray[f]=num_rows[i];
				f++;
			}		
		}
		else
		{			
			for (int i=0; i< n ;i++)
			{
				finalArray[f]=num_rows[i];
				f++;
			}
			for( int i = 0; i< h; i++)
			{
				finalArray[f] = null_rows[i];
				f++;				
			}
		}


		printf("printing the final array AFTER ORDER BY CHAR\n");

		for (int i=1; i<= n+h ;i++)
		{
			//printf("row index:%d::\n",finalArray[i]);			
		}
		
	} // sort char

	//now sort int values

	if(col_type == T_INT)
	{
		int temp1;
		int temp2;

		//printf("N is %d",n);
		//printf("H is %d",h);

		if (desc == 0)
		{
			for(int i=0;i<n;i++)
			{
				for( int j=i+1;j<n;j++)
				{	
					if(numArray[0][i] > numArray[0][j])
					{
						temp1 = numArray[0][i];
						numArray[0][i]=numArray[0][j]; 
						numArray[0][j]=temp1;        

						temp2= numArray[1][i];
						numArray[1][i]=numArray[1][j]; 
						numArray[1][j]=temp2;   
					}
				}
			}	
		}

		if (desc == 1)
		{
			for(int i=0;i<n;i++)
			{
				for(int j=i+1;j<n;j++)
				{	
					if(numArray[0][i] < numArray[0][j])
					{
						temp1 = numArray[0][i];
						numArray[0][i]=numArray[0][j]; 
						numArray[0][j]=temp1;        

						temp2= numArray[1][i];
						numArray[1][i]=numArray[1][j]; 
						numArray[1][j]=temp2;   
					}
				}
			}

			/* for sorting equal values  */
			/*
			for(int i=0;i<n;i++)
			{
			for(int j=i+1;j<n;j++)
			{	
			if(numArray[0][i] < numArray[0][j])
			{
			temp1 = numArray[0][i];
			numArray[0][i]=numArray[0][j]; 
			numArray[0][j]=temp1;        

			temp2= numArray[1][i];
			numArray[1][i]=numArray[1][j]; 
			numArray[1][j]=temp2;   
			}
			}
			}
			*/
		}

		int f = 1;
		if (desc == 1)
		{
			for( int i = 0; i< h; i++)
			{
				finalArray[f] = nullArray[1][i];
				f++;				
			}

			for (int i=0; i< n ;i++)
			{
				finalArray[f]=numArray[1][i];
				f++;
			}		
		}
		else
		{			
			for (int i=0; i< n ;i++)
			{
				finalArray[f]=numArray[1][i];
				f++;
			}
			for( int i = 0; i< h; i++)
			{
				finalArray[f] = nullArray[1][i];
				f++;				
			}
		}

		//printf("printing the final array AFTER ORDER BY T_INT\n");

		for (int i=1; i<= n+h ;i++)
		{
			//printf("row index:%d::\n",finalArray[i]);			
		}

	}// t-int 

	printf("Total rows in final order by :%d",n+h);
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	if (col_list == NULL)
	{
		//printf("col list null \n");

		numCol = tab_entry->num_columns;

		col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
		for(int i=1; i <= numCol ; i++ )
		{
			
			printf("%s\t\t",col_entry->col_name);
			col_entry++;
		}

		printf("\n--------------------------------------------------------------------------------\n");

		if( (fp = fopen(filename,"rbc")) == NULL) 
		{
			rc = FILE_OPEN_ERROR;
		}

		else
		{
			for(j = 1 ; j <= n+h ; j++)
			{

			fseek(fp, (sizeof(table_file_header) + (header->record_size)*(finalArray[j]-1)) ,SEEK_SET);
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			//	printf("enter 1 for\n");

			for(int i=1; i <= numCol ; i++ )
			{

				//	printf("enter 2 for\n");
				//	printf("print col length::%d\n",col_entry->col_len );

				if(col_entry->col_type == T_CHAR)

				{

					char buffer1[MAX_TOK_LEN] ;
					memset(buffer1, 0, sizeof(buffer1));
					char l;
					//l = (char*)calloc(1,sizeof(char));

					fread(&l,1,1,fp);
					//printf("l:%d:",l);

					if( l == 0)
					{
						//printf("m is o\n");
						fread(buffer1 , sizeof(char)*col_entry->col_len  , 1 , fp);
						printf("-\t\t");


					}
					else
					{

						fread(buffer1 , sizeof(char)*(col_entry->col_len)  , 1 , fp);
						printf("%s\t\t",buffer1);

					}


				}

				if(col_entry->col_type== T_INT)
				{

					char l;
					fread(&l, 1,1,fp);

					if(l == 0)
					{
						int *b;
						b = (int*) calloc(sizeof(int),1);
						fread( b,sizeof(int),1,fp);
						printf("-\t\t");

					}

					else
					{
						int *b;

						b = (int*) calloc(sizeof(int),1);
						fread( b, sizeof(int),1,fp );
						printf("%d\t\t",*b);

					}
				}

				col_entry++;
			} //for columns

			printf("\n");

		}//for rows

	} //file oper

	fflush(fp);
	fclose(fp);

	}
/////////////////////////////////////////////////////////////////////////////////////////
	else
	{
		printf("col list NOT null \n");
		
		col_list = col_list->next;
		column_list *cl;

		cl = col_list;
		column_list *cl2;

		cl2 = col_list;

		while(cl->next != NULL)
		{
			printf("%s\t\t",cl->col_name);
			cl =cl->next;
		}	
		printf("%s\t\t",cl->col_name);

		printf("\n--------------------------------------------------------------------------\n");


		//now start reading these values from file

		if( (fp = fopen(filename,"rbc")) == NULL) 
		{
			rc = FILE_OPEN_ERROR;
		}

		else
		{
		//printf("FINAl SIZE of order by is : %d\n", n+h);

		for(int r = 1 ; r <= n+h ;r++)
		{
			fseek(fp, sizeof(table_file_header) + ((finalArray[r]-1)*(header->record_size)) ,SEEK_SET);
			col_list = cl2;
			int x = 0;
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

			while(col_list->next != NULL)
			{
				//	printf("entering while\n");	
				//	printf("\nname of the col paased in select:%s\n",col_list->col_name);

				x = 0;
				col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
				fseek(fp, sizeof(table_file_header)+ ((finalArray[r]-1)*(header->record_size)) ,SEEK_SET);


				for (int i= 0 ; i< tab_entry->num_columns; i++ )
				{
					//	printf("\n inside for: actual col name: %s",col_entry->col_name);
					//	printf("\n Comparing %s and %s the value is %d",col_list->col_name,col_entry->col_name,strcmpi( col_entry->col_name, col_list->col_name ));

					if (strcmpi( col_entry->col_name, col_list->col_name )== 0)
					{

						//printf("\n inside if : actual col name: %s",col_entry->col_name);
						//printf("\n inside if: actual col type: %d\n",col_entry->col_type);
						//read this value from file

						if(col_entry->col_type == T_CHAR)
						{							
							char buffer[MAX_TOK_LEN];
							memset(buffer,0,sizeof(buffer));						
							fseek(fp,x,SEEK_CUR);

							//fseek(fp,1,SEEK_CUR);

							char m;
							fread(&m,sizeof(char),1,fp);

							if (m == 0)
							{
								fread(buffer,sizeof(char)*col_entry->col_len,1,fp);
								printf("-\t\t");
								col_entry++;
								break;

							}

							else
							{
								fread(buffer,sizeof(char)*col_entry->col_len,1,fp);
								printf("%s\t\t",buffer);
								col_entry++;
								break;

							}

						}

						else
						{	int *p;
							p = (int*)calloc(1,sizeof(int));
							fseek(fp,x,SEEK_CUR);
						//	fseek(fp,1,SEEK_CUR);

						char m;
						fread(&m,1,1,fp);

						if(m == 0)
						{
							fread(p,sizeof(int),1,fp);
							printf("-\t\t");
							col_entry++;
							free(p);
							break;

						}

						else
						{
							fread(p,sizeof(int),1,fp);
							printf("%d\t\t",*p);
							col_entry++;
							free(p);
							break;

						}


						}

					}//if col is there 


					// else move to next col
					else	
					{
						//printf("entering else if no match\n");				
						x += (col_entry->col_len +1 );
						col_entry++;
					}


				}//for col ends here

				col_list = col_list->next;

			}//while ends

			x = 0;
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			fseek(fp, sizeof(table_file_header)+ (finalArray[r]-1)*(header->record_size) ,SEEK_SET);

			// printing last value...

			for (int i=0; i< tab_entry->num_columns; i++ )

			{				

				//	printf("\n inside for: actual col name: %s",col_entry->col_name);
				//	printf("\n Comparing %s and %s the value is %d",col_list->col_name,col_entry->col_name,strcmpi( col_entry->col_name, col_list->col_name ));
				if (strcmpi( col_entry->col_name, col_list->col_name )== 0)
				{

					//		printf("\n inside if : actual col name: %s",col_entry->col_name);
					//		printf("\n inside if: actual col type: %d\n",col_entry->col_type);
					//read this value from file

					if(col_entry->col_type == T_CHAR)
					{

						char buffer[MAX_TOK_LEN];
						memset(buffer, 0, sizeof(buffer));
						fseek(fp,x,SEEK_CUR);

						char m;
						fread(&m, sizeof(char) , 1, fp);
						//fseek(fp,1,SEEK_CUR);

						if(m ==0)
						{
							fread(buffer,col_entry->col_len,1,fp);
							printf("-\t\t");
							col_entry++;
							break;

						}

						else
						{
							fread(buffer,col_entry->col_len,1,fp);
							printf("%s\t\t",buffer);
							col_entry++;
							break;

						}

					}

					else
					{	
						int *p;
					p = (int*)calloc(1,sizeof(int));						
					fseek(fp,x,SEEK_CUR);
					//fseek(fp,1,SEEK_CUR);

					char m;
					fread(&m,sizeof(char),1,fp);

					if (m ==0)
					{
						fread(p,sizeof(int),1,fp);
						printf("-\t\t");
						col_entry++;
						free(p);
						break;

					}

					else
					{
						fread(p,sizeof(int),1,fp);
						printf("%d\t\t",*p);
						col_entry++;
						free(p);
						break;


					}
					}

				}//if col is there 


				// else move to next col
				else

				{
					//printf("entering else if no match for last element\n");
					x += (col_entry->col_len +1 );						
					col_entry++;
				}


			}//for col ends here

			printf("\n");

		}//for rows ends here

		printf("%s size is :%d\n",filename,header->file_size);

		fflush(fp);
		fclose(fp);


	}//file error


	} // col n\list not null

	} // where list null


//////////////////////////////////////////////////////////////////////////////////////////////////////////
	else
	{	
		//printf("nupur CREATE\n");
		res_array = create_where_list(where_col_list, tab , &size,0,0 );
	

	printf("size NUPUR is: %d\n", size);
	//printf("BEFORE ORDER BY\n");

	for(int i = 0 ; i < size; i++)
	{
		printf("index value %d:%d\n",(i+1),*(res_array + i)); 

	}

	char stringArray[50][MAX_IDENT_LEN];
	int num_rows[50];
	int null_rows[50];

	int numArray[2][1000];
	int nullArray[2][1000];
	int n = 0;
	int h = 0;
	int finalArray[1000];


	tpd_entry *tab_entry;
	tab_entry = get_tpd_from_list(tab);

	int x = 0;
	int j;
	int numCol = tab_entry->num_columns;

	cd_entry *col_entry;
	col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

/*
	char filename[20] ;
	strcpy(filename , tab);
	strcat(filename, ".tab");

	FILE *fp;
*/
	for (int i = 1; i<= tab_entry->num_columns;i++)
	{
		if (strcmpi(col_entry->col_name, order_by_col_name) == 0)
		{
			//printf("col match\n");
			break;		
		}
		else
		{

			x = x + (col_entry->col_len +1);
			col_entry++;

		}

	} //for col ends here..

	if( (fp = fopen(filename,"rbc")) == NULL) 
	{
		rc = FILE_OPEN_ERROR;
	}

	else
	{
		table_file_header *header;
		header = (table_file_header*)calloc(1, sizeof(table_file_header));
		fread(header ,sizeof(table_file_header) ,1,fp);



		for (int j = 1; j <= size ;j++)
		{
			fseek(fp, (sizeof(table_file_header) +(header->record_size)*(*(res_array + (j-1))- 1 )), SEEK_SET);
			fseek(fp, x ,SEEK_CUR);

			if(col_type== T_CHAR)
			{
				//printf("entreng TCHAR\n");

				//printf("entreng TCHAR\n");
				char m;
				fread(&m,1,1,fp);
				char buffer[MAX_IDENT_LEN];
				//int p;
				fread(buffer, col_entry->col_len ,1, fp);

				if ( m != 0)
				{
					//printf("XXXX!!");
					//printf("p is:%d\n",p);
					//numArray[0][n] = p;
					strcpy(stringArray[n] ,buffer);
					num_rows[n] = *(res_array + j-1);
					n++;
					//printf("n :%d",n);

				}
				else
				{
					null_rows[h] = *(res_array + j-1);
					h++;					
				}

			} //T-_CHAR

			if(col_entry->col_type==T_INT)
			{

				char m;
				fread(&m,1,1,fp);
				int p;
				fread(&p,sizeof(int),1,fp);
				//printf(

				if ( m != 0)
				{
					//printf("p is:%d\n",p);
					numArray[0][n] = p;
					//numArray[n][1]= rowIndex[j-1];
					//printf("row index nupur:%d\n",*(res_array + j-1));
					numArray[1][n] = *(res_array + j-1);
					//numArray[1][n] = j;
					n++;
				}

				else
				{
					//nullArray[h][1]=rowIndex[j-1];
					//nullArray[h][0] = -1;
					nullArray[1][h] = *(res_array + j-1);
					//nullArray[1][h] = j;
					h++;

				}

			} //T_int ends here

		} // for rows end here

		
		fflush(fp);
		fclose(fp);

	} // file error


	// sort T_char values

	if( col_type == T_CHAR)

	{
		int temp1;

		printf("L\n");		

		printf("N is %d",n);
		printf("H is %d",h);

		for(int i = 0 ;i<n ;i++)
		{
			//printf("N");		
			//printf("%s",stringArray[i]);
		}

		if (desc == 0)
		{
			for(int i = 0; i< n ;i++)
			{
				for(int j = i+1; j< n ;j++)
				{
					if (strcmp(stringArray[i],stringArray[j]) > 0)
					{
						//printf("DD\n");
						temp1 = num_rows[i];
						num_rows[i] = num_rows[j];
						num_rows[j] = temp1;
						
					}
		
				}
		
			}

		}

		if (desc == 1)
		{
			for(int i = 0; i< n ;i++)
			{
				for(int j =i+1; j< n ;j++)
				{

					if (strcmp(stringArray[i],stringArray[j]) < 0)
					{

						temp1 = num_rows[i];
						num_rows[i] = num_rows[j];
						num_rows[j] = temp1;
			
					}		
				}		
			}
		}
		//////////////////////////
		int f = 0;
		if (desc == 1)
		{
			for( int i = 0; i< h; i++)
			{
				finalArray[f] = null_rows[i];
				f++;				
			}

			for (int i=0; i< n ;i++)
			{
				finalArray[f]=num_rows[i];
				f++;
			}		
		}
		else
		{			
			for (int i=0; i< n ;i++)
			{
				finalArray[f]=num_rows[i];
				f++;
			}
			for( int i = 0; i< h; i++)
			{
				finalArray[f] = null_rows[i];
				f++;				
			}
		}


		printf("printing the final array AFTER ORDER BY CHAR\n");

		for (int i = 0; i < n+h ;i++)
		{
			//printf("row index:%d::\n",finalArray[i]);			
		
	
	}

	} // sort t-char

	//now sort int values

	if ( col_type == T_INT)
	{
	
	int temp1;
	int temp2;

	if (desc == 0)
	{
		for(int i=0;i<n;i++)
		{
			for( int j=i+1;j<n;j++)
			{	
				if(numArray[0][i] > numArray[0][j])
				{
					temp1 = numArray[0][i];
					numArray[0][i]=numArray[0][j]; 
					numArray[0][j]=temp1;        

					temp2= numArray[1][i];
					numArray[1][i]=numArray[1][j]; 
					numArray[1][j]=temp2;   
				}
			}
		}	
	}

	if (desc == 1)
	{
		for(int i=0;i<n;i++)
		{
			for(int j=i+1;j<n;j++)
			{	
				if(numArray[0][i] < numArray[0][j])
				{
					temp1 = numArray[0][i];
					numArray[0][i]=numArray[0][j]; 
					numArray[0][j]=temp1;        

					temp2= numArray[1][i];
					numArray[1][i]=numArray[1][j]; 
					numArray[1][j]=temp2;   
				}
			}
		}

		/* for sorting equal values  */
		/*
		for(int i=0;i<n;i++)
		{
		for(int j=i+1;j<n;j++)
		{	
		if(numArray[0][i] < numArray[0][j])
		{
		temp1 = numArray[0][i];
		numArray[0][i]=numArray[0][j]; 
		numArray[0][j]=temp1;        

		temp2= numArray[1][i];
		numArray[1][i]=numArray[1][j]; 
		numArray[1][j]=temp2;   
		}
		}
		}
		*/
	}

	int f = 0;
	if (desc == 1)
	{
		for( int i = 0; i< h; i++)
		{
			finalArray[f] = nullArray[1][i];
			f++;				
		}

		for (int i=0; i< n ;i++)
		{
			finalArray[f]=numArray[1][i];
			f++;
		}		
	}
	else
	{			
		for (int i=0; i< n ;i++)
		{
			finalArray[f]=numArray[1][i];
			f++;
		}
		for( int i = 0; i< h; i++)
		{
			finalArray[f] = nullArray[1][i];
			f++;				
		}
	}
	printf("printing the final array AFTER ORDER BY\n");

	for (int i= 0; i< n+h ;i++)
	{
		//printf("row index:%d::\n",finalArray[i]);			
	}

	
} // sort T-INT

    printf("Total rows in final order by :%d",n+h);

	///////////////////////////////////////////////////////////////////////////////////
	if (col_list == NULL)
	{
		printf("col list null \n");

		numCol = tab_entry->num_columns;

		col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
		for(int i=1; i <= numCol ; i++ )
		{
			
			printf("%s\t\t",col_entry->col_name);
			col_entry++;
		}

		printf("\n--------------------------------------------------------------------------------\n");

		if( (fp = fopen(filename,"rbc")) == NULL) 
		{
			rc = FILE_OPEN_ERROR;
		}

		else
		{
			for(j = 1 ; j <= n+h ; j++)
			{

			fseek(fp, (sizeof(table_file_header) + (header->record_size)*(finalArray[j-1]-1)) ,SEEK_SET);
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			//	printf("enter 1 for\n");

			for(int i=1; i <= numCol ; i++ )
			{

				//	printf("enter 2 for\n");
				//	printf("print col length::%d\n",col_entry->col_len );

				if(col_entry->col_type == T_CHAR)

				{

					char buffer1[MAX_TOK_LEN] ;
					memset(buffer1, 0, sizeof(buffer1));
					char l;
					//l = (char*)calloc(1,sizeof(char));

					fread(&l,1,1,fp);
					//printf("l:%d:",l);

					if( l == 0)
					{
						//printf("m is o\n");
						fread(buffer1 , sizeof(char)*col_entry->col_len  , 1 , fp);
						printf("-\t\t");


					}
					else
					{

						fread(buffer1 , sizeof(char)*(col_entry->col_len)  , 1 , fp);
						printf("%s\t\t",buffer1);

					}


				}

				if(col_entry->col_type== T_INT)
				{

					char l;
					fread(&l, 1,1,fp);

					if(l == 0)
					{
						int *b;
						b = (int*) calloc(sizeof(int),1);
						fread( b,sizeof(int),1,fp);
						printf("-\t\t");

					}

					else
					{
						int *b;

						b = (int*) calloc(sizeof(int),1);
						fread( b, sizeof(int),1,fp );
						printf("%d\t\t",*b);

					}
				}

				col_entry++;
			} //for columns

			printf("\n");

		}//for rows

	} //file oper

	fflush(fp);
	fclose(fp);

	}
/////////////////////////////////////////////////////////////////////////////////////////
	else
	{
		printf("col list NOT null \n");
		
		col_list = col_list->next;
		column_list *cl;

		cl = col_list;
		column_list *cl2;

		cl2 = col_list;

		while(cl->next != NULL)
		{
			printf("%s\t\t",cl->col_name);
			cl =cl->next;
		}	
		printf("%s\t\t",cl->col_name);

		printf("\n--------------------------------------------------------------------------\n");


		//now start reading these values from file

		if( (fp = fopen(filename,"rbc")) == NULL) 
		{
			rc = FILE_OPEN_ERROR;
		}

		else
		{
		//printf("FINAl SIZE of order by is : %d\n", n+h);

		for(int r = 0 ; r < n+h ;r++)
		{
			fseek(fp, sizeof(table_file_header) + ((finalArray[r]-1)*(header->record_size)) ,SEEK_SET);
			col_list = cl2;
			int x = 0;
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

			while(col_list->next != NULL)
			{
				//	printf("entering while\n");	
				//	printf("\nname of the col paased in select:%s\n",col_list->col_name);

				x = 0;
				col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
				fseek(fp, sizeof(table_file_header)+ ((finalArray[r]-1)*(header->record_size)) ,SEEK_SET);


				for (int i= 0 ; i< tab_entry->num_columns; i++ )
				{
					//	printf("\n inside for: actual col name: %s",col_entry->col_name);
					//	printf("\n Comparing %s and %s the value is %d",col_list->col_name,col_entry->col_name,strcmpi( col_entry->col_name, col_list->col_name ));

					if (strcmpi( col_entry->col_name, col_list->col_name )== 0)
					{

						//printf("\n inside if : actual col name: %s",col_entry->col_name);
						//printf("\n inside if: actual col type: %d\n",col_entry->col_type);
						//read this value from file

						if(col_entry->col_type == T_CHAR)
						{							
							char buffer[MAX_TOK_LEN];
							memset(buffer,0,sizeof(buffer));						
							fseek(fp,x,SEEK_CUR);

							//fseek(fp,1,SEEK_CUR);

							char m;
							fread(&m,sizeof(char),1,fp);

							if (m == 0)
							{
								fread(buffer,sizeof(char)*col_entry->col_len,1,fp);
								printf("-\t\t");
								col_entry++;
								break;

							}

							else
							{
								fread(buffer,sizeof(char)*col_entry->col_len,1,fp);
								printf("%s\t\t",buffer);
								col_entry++;
								break;

							}

						}

						else
						{	int *p;
							p = (int*)calloc(1,sizeof(int));
							fseek(fp,x,SEEK_CUR);
						//	fseek(fp,1,SEEK_CUR);

						char m;
						fread(&m,1,1,fp);

						if(m == 0)
						{
							fread(p,sizeof(int),1,fp);
							printf("-\t\t");
							col_entry++;
							free(p);
							break;

						}

						else
						{
							fread(p,sizeof(int),1,fp);
							printf("%d\t\t",*p);
							col_entry++;
							free(p);
							break;

						}


						}

					}//if col is there 


					// else move to next col
					else	
					{
						//printf("entering else if no match\n");				
						x += (col_entry->col_len +1 );
						col_entry++;
					}


				}//for col ends here

				col_list = col_list->next;

			}//while ends

			x = 0;
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			fseek(fp, sizeof(table_file_header)+ (finalArray[r]-1)*(header->record_size) ,SEEK_SET);

			// printing last value...

			for (int i=0; i< tab_entry->num_columns; i++ )

			{				

				//	printf("\n inside for: actual col name: %s",col_entry->col_name);
				//	printf("\n Comparing %s and %s the value is %d",col_list->col_name,col_entry->col_name,strcmpi( col_entry->col_name, col_list->col_name ));
				if (strcmpi( col_entry->col_name, col_list->col_name )== 0)
				{

					//		printf("\n inside if : actual col name: %s",col_entry->col_name);
					//		printf("\n inside if: actual col type: %d\n",col_entry->col_type);
					//read this value from file

					if(col_entry->col_type == T_CHAR)
					{

						char buffer[MAX_TOK_LEN];
						memset(buffer, 0, sizeof(buffer));
						fseek(fp,x,SEEK_CUR);

						char m;
						fread(&m, sizeof(char) , 1, fp);
						//fseek(fp,1,SEEK_CUR);

						if(m ==0)
						{
							fread(buffer,col_entry->col_len,1,fp);
							printf("-\t\t");
							col_entry++;
							break;

						}

						else
						{
							fread(buffer,col_entry->col_len,1,fp);
							printf("%s\t\t",buffer);
							col_entry++;
							break;

						}

					}

					else
					{	int *p;
					p = (int*)calloc(1,sizeof(int));						
					fseek(fp,x,SEEK_CUR);
					//fseek(fp,1,SEEK_CUR);

					char m;
					fread(&m,sizeof(char),1,fp);

					if (m ==0)
					{
						fread(p,sizeof(int),1,fp);
						printf("-\t\t");
						col_entry++;
						free(p);
						break;

					}

					else
					{
						fread(p,sizeof(int),1,fp);
						printf("%d\t\t",*p);
						col_entry++;
						free(p);
						break;


					}
					}

				}//if col is there 


				// else move to next col
				else

				{
					//printf("entering else if no match for last element\n");
					x += (col_entry->col_len +1 );						
					col_entry++;
				}


			}//for col ends here

			printf("\n");

		}//for rows ends here

		printf("%s size is :%d\n",filename,header->file_size);

		fflush(fp);
		fclose(fp);


	}//file error


	} // col n\list not null

}//where_list present


	return rc;

}



int* create_where_list(where_list *where_col_list, char *tab , int *s, int del_call = 0 ,int *d = 0)
{
	int delete_call = del_call;

	printf("Entering Create_where_list\n");
	static int finalArr[1000];
	printf("entering selct all for where clause\n");
	int size = 0;
	int index = 0;
	printf("entering selct all for where clause\n");

	where_col_list = where_col_list->next;
	/*
	while(where_col_list->next != NULL)
	{
	printf("%s\n",where_col_list->col_name);
	printf("%d\n",where_col_list->null_check);
	printf("%d\n",where_col_list->arith_operator);
	printf("%s\n",where_col_list->value);
	printf("%d\n",where_col_list->col_type);		
	printf("%d\n",where_col_list->relational_operator);

	where_col_list = where_col_list->next;

	}

	printf("%s\n",where_col_list->col_name);
	printf("%d\n",where_col_list->null_check);
	printf("%d\n",where_col_list->arith_operator);
	printf("%s\n",where_col_list->value);
	printf("%d\n",where_col_list->col_type);		
	printf("%d\n",where_col_list->relational_operator);

	*/
	int rc =0;
	char filename[20] ;
	strcpy(filename , tab);
	strcat(filename, ".tab");
	//	printf("%s\n",filename);

	tpd_entry *tab_entry;
	tab_entry = get_tpd_from_list(tab);

	//first get the table and open that corresponding file

	FILE *fp;
	fp = fopen(filename, "rbc");
	int i = 1;

	struct _stat file_stat;
	_fstat(_fileno(fp), &file_stat);

	//printf("%s size is: %d\n",filename ,file_stat.st_size);


	if(fp == NULL)
	{

		printf("\n file OPEN error\n");

		rc = FILE_OPEN_ERROR;
		//return 

	}

	else
	{

		//first read the num of records from ur header

		table_file_header *header;
		header = (table_file_header*)calloc(1, sizeof(table_file_header));
		fread(header ,sizeof(table_file_header) ,1,fp);

		cd_entry *col_entry;	
		int numCol;
		int j;
		numCol = tab_entry->num_columns;

		fseek(fp,sizeof(table_file_header), SEEK_SET);
		col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

		//	printf("enter 1 for\n");

		printf("MAIN FUNCTION\n");

		//	while(where_col_list->next != NULL)
		//	{
		printf("entering while\n");
		int x = 0;
		int rowIndex[1000];
		int r = 0;
		int rowIndex2[1000];
		int q = 0;
		int rel_op;
		int notFoundIndex[1000];
		int a = 0;

		printf("link list col name :%s\n",where_col_list->col_name);

		for(i = 1; i <= numCol ; i++ )

		{
			printf("entering for\n");
			if(strcmpi (where_col_list->col_name ,col_entry->col_name) == 0)

			{						
				printf("col found\n");
				fseek(fp , x, SEEK_CUR);

				if (where_col_list->null_check == -1)
				{
					if(where_col_list->col_type == col_entry->col_type)
					{
						if( where_col_list->col_type == T_CHAR )
						{
							char buffer[MAX_IDENT_LEN];
							memset(buffer ,0 ,sizeof(buffer)); 
							//fseek(fp,1,SEEK_CUR);
							char m;

							for(j=1; j <=header->num_records; j++)
							{
								printf("entering for row loop\n");	
								fread(&m,1,1,fp);															
								fread(buffer,col_entry->col_len, 1, fp );	
								printf("%s\n",buffer);

								if(where_col_list->arith_operator== S_EQUAL) 
								{
								
									printf("Equal symbol\n");

									if(m != 0)
									{
										if(strcmp(buffer, where_col_list->value) == 0)
										{									
											printf("found data \n");
											rowIndex[r] = j;
											r++;

										}

										else
										{
											printf("Didn't find data\n");
											notFoundIndex[a] = j;
											a++;
							
										}
									}

									else
									{
										printf("Didn't find data\n");
										notFoundIndex[a] = j;
										a++;
								
								
									}

								}

								else if(where_col_list->arith_operator== S_LESS)
								{
									printf("Less symbol\n");

									if(m != 0)
									{
										if(strcmp(buffer, where_col_list->value) < 0)
										{										
											printf("found data \n");
											rowIndex[r] = j;
											r++;

										}

										else
										{
											printf("Didn't find data\n");
											notFoundIndex[a] = j;
											a++;
							
										}
									}

									else
									{
										printf("Didn't find data\n");
										notFoundIndex[a] = j;
										a++;
								
								
									}							
								
								} // Less
								
								else if(where_col_list->arith_operator== S_GREATER)
								{
									printf("More symbol\n");

									if(m != 0)
									{
										if(strcmp(buffer, where_col_list->value) > 0)
										{										
											printf("found data \n");
											rowIndex[r] = j;
											r++;

										}

										else
										{
											printf("Didn't find data\n");
											notFoundIndex[a] = j;
											a++;
							
										}
									}

									else
									{
										printf("Didn't find data\n");
										notFoundIndex[a] = j;
										a++;
								
								
									}	
								} //more

								int new_seek_length = (header->record_size - col_entry->col_len) - 1;
								fseek( fp, new_seek_length, SEEK_CUR);

							} //for rows

							printf("r is:%d\n",r);
							printf("beofr free\n");

							printf("after free\n");
						}

						else if( where_col_list->col_type == T_INT)

						{

							printf("numeric data\n");
							int *p;
							p = (int*)calloc(1,sizeof(int));

							char m;

							for(j=1; j <=header->num_records; j++)
							{
								printf("entering for row loop\n");		
								fread(&m,1,1,fp);
								fread(p, sizeof(int), 1, fp );	
								printf("%d\n",*p);

								if(where_col_list->arith_operator== S_EQUAL) 
								{
									printf("Equal symbol\n");

									if(m !=0)
									{

										if(*p == atoi(where_col_list->value) )
										{										
											printf("found data \n");
											rowIndex[r] = j;
											r++;

										}
										else
										{
											printf("Didn't find data\n");
											notFoundIndex[a] = j;
											a++;
										
										}
									}

									else
									{
										printf("Didn't find data\n");
										notFoundIndex[a] = j;
										a++;
																	
									}

								}

								else if((where_col_list->arith_operator== S_LESS))
								{
									printf("Less symbol\n");

									if(m !=0)
									{

										if(*p < atoi(where_col_list->value) )
										{										
											printf("found data \n");
											rowIndex[r] = j;
											r++;

										}
										else
										{
											printf("Didn't find data\n");
											notFoundIndex[a] = j;
											a++;
										
										}
									}

									else
									{
										printf("Didn't find data\n");
										notFoundIndex[a] = j;
										a++;
																	
									}

								}
								else if((where_col_list->arith_operator== S_GREATER))
								{
									printf("More symbol\n");

									if(m != 0)
									{

										if(*p > atoi(where_col_list->value) )
										{										
											printf("found data \n");
											rowIndex[r] = j;
											r++;

										}
										else
										{
											printf("Didn't find data\n");
											notFoundIndex[a] = j;
											a++;
										
										}

									}
								}

								int new_seek_length = (header->record_size - sizeof(int)) - 1;
								fseek( fp, new_seek_length, SEEK_CUR);

							} //for rows

							printf("r is:%d\n",r);
							free(p);

						}
					} //data mismatch

					else								
					{							
						printf("data mismatch error\n");
						rc = DATATYPE_MISMATCH_ERROR;
						//break;
						//return rc;

					}

				} //null check absent

				else
				{
					if(where_col_list->null_check ==1)
					{
						printf(" null condition\n");

						//fseek(fp,1,SEEK_CUR);
						char *m;
						m = (char*)calloc(1,sizeof(char));
						for(j=1; j <=header->num_records; j++)
						{
							printf("entering for row loop\n");								
							fread(m,1, 1, fp );									
							printf("%d\n",*m);

							if(*m == 0)
							{										
								printf("found data \n");
								rowIndex[r] = j;
								r++;

							}
							else
							{
								printf("Didn't find data\n");
								notFoundIndex[a] = j;
								a++;
										
							}
							int new_seek_length = (header->record_size - 1);
							fseek( fp, new_seek_length, SEEK_CUR);

						} //for rows

						printf("r is:%d\n",r);
						free(m);
					}					
					else
					{
						printf(" NOT null condition\n");
						char *m;
						m = (char*)calloc(1,sizeof(char));

						for(j=1; j <=header->num_records; j++)
						{
							printf("entering for row loop\n");								
							fread(m,1, 1, fp );									
							printf("%d\n",*m);

							if(*m != 0)
							{										
								printf("found data \n");
								rowIndex[r] = j;
								r++;

							}
							else
							{
								printf("Didn't find data\n");
								notFoundIndex[a] = j;
								a++;
										
							}
							int new_seek_length = (header->record_size - 1);
							fseek( fp, new_seek_length, SEEK_CUR);

						} //for rows

						printf("r is:%d\n",r);
						free(m);

					}
				} // null check present

				break;

			} //if col name found

			else
			{

				printf("column not found\n");
				x = x + col_entry->col_len+1;
				col_entry++;
			}

		}//for columns

		//		} // while

		if (where_col_list->relational_operator == EOC)
		{
			printf("only one where condition\n");

			if ( delete_call == 1)
			{
				if( r != 0)
				{
					for (int i = 0 ; i<a ; i++)
					{
						finalArr[size] = notFoundIndex[i];
						size++;
					}		
				}
				else
				{
					//printf("No ROWS FOUND based on WHERE condition!!\n");
					index = -1;
					*d = index;

								
				}
			}
			else
			{
				printf("HIIII\n");
				for (int i = 0 ; i<r ; i++)
				{
					//printf("HIIII\n");
					finalArr[size] = rowIndex[i];
					size++;
					//printf("HIIII\n");
				}	
				printf("HIIII\n");
			}
		}

		else
		{
			rel_op = where_col_list->relational_operator;

			printf("Two where condition\n");

			where_col_list=where_col_list->next;

			fseek(fp,sizeof(table_file_header), SEEK_SET);
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

			printf("entering while\n");
			x =0;
			//int rowIndex2[1000];
			//r = 0;

			printf("link list col name :%s\n",where_col_list->col_name);
			for(i = 1; i <= numCol ; i++ )

			{
				printf("entering for\n");
				if(strcmpi (where_col_list->col_name ,col_entry->col_name) == 0)

				{						
					printf("col found\n");
					fseek(fp , x, SEEK_CUR);

					if (where_col_list->null_check == -1)
					{
						if(where_col_list->col_type == col_entry->col_type)
						{
							if( where_col_list->col_type == T_CHAR )
							{
								char buffer[MAX_IDENT_LEN];
								memset(buffer ,0 ,sizeof(buffer)); 
								//fseek(fp,1,SEEK_CUR);
								char m;
								
								for(j=1; j <=header->num_records; j++)
								{
									printf("entering for row loop\n");		
									fread(&m,1,1,fp);
									fread(buffer,col_entry->col_len, 1, fp );	
									printf("%s\n",buffer);

									if(where_col_list->arith_operator == S_EQUAL) 
									{
										printf("Equal symbol\n");

										if(m!=0)
										{
											if(strcmp(buffer, where_col_list->value) == 0)
											{										
												printf("found data \n");
												rowIndex2[q] = j;
												q++;

											}
										}
									}

									else if(where_col_list->arith_operator == S_LESS) 
									{
										printf("Less symbol\n");

										if(m!=0)
										{
											if(strcmp(buffer, where_col_list->value) < 0)
											{										
												printf("found data \n");
												rowIndex2[q] = j;
												q++;

											}
										}
									}

									else if(where_col_list->arith_operator == S_GREATER) 
									{
										printf("More symbol\n");

										if(m != 0)
										{
											if(strcmp(buffer, where_col_list->value) > 0)
											{										
												printf("found data \n");
												rowIndex2[q] = j;
												q++;

											}
										}
									}

									int new_seek_length = (header->record_size - col_entry->col_len) - 1;
									fseek( fp, new_seek_length, SEEK_CUR);

								} //for rows

								printf("q is:%d\n",q);
								printf("beofr free\n");

								printf("after free\n");
							}

							else if( where_col_list->col_type == T_INT)

							{

								printf("numeric data\n");
								int *p;
								p = (int*)calloc(1,sizeof(int));

								//fseek(fp,1,SEEK_CUR);
								char m;


								for(j=1; j <=header->num_records; j++)
								{
									printf("entering for row loop\n");
									fread(&m,1,1,fp);
									fread(p, sizeof(int), 1, fp );	
									printf("%d\n",*p);

									if(where_col_list->arith_operator== S_EQUAL) 
									{
										printf("Equal symbol\n");

										if(m!=0)
										{

											if(*p == atoi(where_col_list->value) )
											{										
												printf("found data \n");
												rowIndex2[q] = j;
												q++;

											}
										}
									}

									else if((where_col_list->arith_operator== S_LESS))
									{
										printf("Less symbol\n");

										if(m!=0)
										{

											if(*p < atoi(where_col_list->value) )
											{										
												printf("found data \n");
												rowIndex2[q] = j;
												q++;

											}
										}

									}
									else if((where_col_list->arith_operator== S_GREATER))
									{
										printf("More symbol\n");
										if(m !=0)
										{

											if(*p > atoi(where_col_list->value) )
											{										
												printf("found data \n");
												rowIndex2[q] = j;
												q++;

											}
										}														
									}

									int new_seek_length = (header->record_size - sizeof(int)) - 1;
									fseek( fp, new_seek_length, SEEK_CUR);

								} //for rows

								printf("q is:%d\n",q);
								free(p);

							}
						} //data mismatch

						else								
						{							
							printf("data mismatch error\n");
							rc = DATATYPE_MISMATCH_ERROR;
							//break;
							//return rc;

						}

					} //null check absent

					else
					{
						if(where_col_list->null_check ==1)
						{
							printf(" null condition\n");

							//fseek(fp,1,SEEK_CUR);
							char *m;
							m = (char*)calloc(1,sizeof(char));
							for(j=1; j <=header->num_records; j++)
							{
								printf("entering for row loop\n");								
								fread(m,1, 1, fp );									
								printf("%d\n",*m);

								if(*m == 0)
								{										
									printf("found data \n");
									rowIndex2[q] = j;
									q++;

								}
								int new_seek_length = (header->record_size - 1);
								fseek( fp, new_seek_length, SEEK_CUR);

							} //for rows

							printf("q is:%d\n",q);
							free(m);



						}					
						else
						{
							printf(" NOT null condition\n");
							char *m;
							m = (char*)calloc(1,sizeof(char));

							for(j=1; j <=header->num_records; j++)
							{
								printf("entering for row loop\n");								
								fread(m,1, 1, fp );									
								printf("%d\n",*m);

								if(*m != 0)
								{										
									printf("found data \n");
									rowIndex2[q] = j;
									q++;

								}
								int new_seek_length = (header->record_size - 1);
								fseek( fp, new_seek_length, SEEK_CUR);

							} //for rows

							printf("q is:%d\n",q);
							free(m);

						}
					} // null check present

					break;

				} //if col name found

				else
				{

					printf("column not found\n");
					x = x + col_entry->col_len+1;
					col_entry++;
				}

			}//for columns

		} // 2nd where cond

		if(rel_op == K_OR)
		{
			printf("before union\n");
			i = 0;
			j = 0;

			while(i < r && j < q)
			{

				if(rowIndex[i] < rowIndex2[j])
				{
					printf(" %d ", rowIndex[i]);
					finalArr[size] = rowIndex[i];
					i++;
					size++;
				}

				else if(rowIndex2[j] < rowIndex[i])
				{
					printf(" %d ", rowIndex2[j]);
					finalArr[size] = rowIndex2[j];
					j++;
					size++;

				}
				else
				{
					//printf("enter 3 loop\n");
					printf(" %d ", rowIndex2[j]);
					finalArr[size] = rowIndex2[j];
					//printf(" %d ", finalArr[p] );
					size++;
					j++;
					i++;
				}
			}

			/* Print remaining elements of the larger array */

			while(i < r)
			{
				printf(" %d ", rowIndex[i]);
				finalArr[size] = rowIndex[i];
				//printf(" %d ", finalArr[p] );
				i++;
				size++;

			}
			while(j < q)
			{

				printf(" %d ", rowIndex2[j]);
				finalArr[size] = rowIndex2[j];
				//printf(" %d ", finalArr[p] );
				j++;
				size++;
			}

			printf("after union\n");

			for(int u=0;u<size;u++)
			{	 
				printf("printing union array\n");
				printf("%d:",finalArr[u]);
			}	
		} // rel oper for OR


		if (rel_op == K_AND)
		{
			int i = 0, j = 0;
			while ((i < r) && (j < q))
			{    
				if (rowIndex[i] < rowIndex2[j])
				{
					i++;
				}

				else if (rowIndex[i] > rowIndex2[j])
				{
					j++;
				}

				else
				{
					finalArr[size] = rowIndex[i];
					i++;
					j++;
					size++;
				}
			}		

			for(int u=0;u<size;u++)
			{	 
				printf("printing intersection array\n");
				printf("%d:",finalArr[u]);
			}


			//return finalArr;
		} // for rel oper AND

	} //file oper

	fflush(fp);
	fclose(fp);

	printf("HIIII\n");
	*s = size;
	printf("MY\n");
	printf("index:%d",index);
	//*d = index; 
	printf("HIIII\n");
	printf("returning from create_where_list\n"); 
	return finalArr;
}


int delete_from_file_where_clause(where_list *where_col_list ,char *tab)

{
	printf("ENTERING DELETE WHERE CLAUSE\n");
	int rc = 0;
	char *filename = NULL;
	filename = (char*)calloc(MAX_TOK_LEN,sizeof(char));
	strcpy(filename,tab);
	strcat(filename, ".tab");
	table_file_header *header;
	FILE *fp;

	int o_size;
	tpd_entry *tab_entry;
	tab_entry = get_tpd_from_list(tab);
	cd_entry *col_entry;
	int numCol = tab_entry->num_columns;

	int size = 0;
	int a = 0;
	int *res_array;
	
	res_array = create_where_list(where_col_list, tab , &size ,1 ,&a);
	//printf("a:%d",a);

	if( a != -1)
	{

	record_new_entry *record_list = NULL;
	record_list = (record_new_entry*)calloc(1,sizeof(record_new_entry) );

	//printf("size NUPUR is: %d\n", size);
	//printf("BEFORE ORDER BY\n");

	for(int i = 0 ; i < size; i++)
	{
		//printf("index value %d:%d\n",(i+1),*(res_array + i)); 

	}
	
	fp = fopen( filename ,"rbc");
	if(fp == NULL)
	{
		//error
		rc = FILE_OPEN_ERROR;
		printf("file error\n");
	}

	else
	{
		header = (table_file_header*)calloc(1,sizeof( table_file_header));
		fread(header, sizeof(table_file_header),1,fp);
		printf("Old SIZE %d,%d,%d,%d,%d\n",header->file_size,header->record_size,header->num_records,header->record_offset,header->file_header_flag);

		o_size = header->num_records;
		for(int j = 1; j <= size ; j++)
		{

			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			fseek(fp, (sizeof(table_file_header) +(header->record_size)*(*(res_array + (j-1))- 1 )), SEEK_SET);

			for(int i=1; i <= numCol ; i++ )
			{

				//	printf("enter 2 for\n");
				//	printf("print col length::%d\n",col_entry->col_len );

				if(col_entry->col_type == T_CHAR)

				{

					char buffer1[MAX_TOK_LEN] ;
					memset(buffer1, 0, sizeof(buffer1));
					char m;
					fread(&m,1,1,fp);

					if( m == 0)
					{									
						fread(buffer1 , sizeof(char)*col_entry->col_len  , 1 , fp);
						//printf("%s\t\t",buffer1);
						//printf("-\t\t");
						add_to_recordList( &record_list, buffer1 ,col_entry->col_len, col_entry->col_type);

					}
					else
					{
						fread(buffer1 , sizeof(char)*(col_entry->col_len)  , 1 , fp);
						//printf("%s\t\t",buffer1);
						add_to_recordList( &record_list, buffer1 ,col_entry->col_len, col_entry->col_type);
					}

				}

				if(col_entry->col_type== T_INT)
				{
					char buffer1[MAX_TOK_LEN] ;
					memset(buffer1, 0, sizeof(buffer1));
					char m;
					fread(&m, 1,1,fp);


					if(m == 0)
					{
						int *p;
						p = (int*) calloc(sizeof(int),1);
						fread( p,sizeof(int),1,fp);
						//itoa(*p,buffer1,10);
						strcpy(buffer1, "NULL");
						add_to_recordList( &record_list, buffer1 ,col_entry->col_len, col_entry->col_type);
						//printf("%s\t\t",buffer1);
						//printf("-\t\t");

					}
					else
					{
						int *p;

						p = (int*) calloc(sizeof(int),1);
						fread( p, sizeof(int),1,fp );
						itoa(*p,buffer1,10);
						add_to_recordList( &record_list, buffer1 ,col_entry->col_len, col_entry->col_type);
						//printf("%d\t\t",*p);
					}
				}

				col_entry++;
			} //for col

			//printf("\n");
		}//for rows
	}

	fflush(fp);
	fclose(fp);


	fp = fopen(filename,"wbc");
	if(fp == NULL)
	{	
		rc = FILE_OPEN_ERROR;
		printf("file error\n");

	}

	else
	{
		//printf("Updating the header\n");
		header->num_records = size;
		header->file_size = sizeof(table_file_header) + (header->num_records)*(header->record_size);

		fwrite(header, sizeof(table_file_header),1,fp);

		if(size != 0)
		{
			    record_list = record_list->next;
				while(record_list->next != NULL)
			{
				//fwrite(ptr,1,1,fp);
				if(record_list->cType == T_INT)
				{

					if( strcmpi( record_list->record_string ,"NULL")== 0)
					{
						//printf("check\n");
						char m = 0;
						fwrite(&m,1,1,fp);						
						int a = atoi(record_list->record_string);
						fwrite(&a ,4,1,fp);

					}
					else
					{
						//printf("check2\n");
						int a = atoi(record_list->record_string);
						fwrite(record_list,1,1,fp);
						fwrite(&a ,4,1,fp);

					}
				}
				else
				{
					if( strcmpi(record_list->record_string, "NULL")==0)
					{

						//printf("check3\n");
						record_list->col_len = 0;
						fwrite(record_list,record_list->max_length +1,1,fp);
					}

					else
					{
						//printf("check4\n");
						fwrite(record_list,record_list->max_length +1,1,fp);
					}
				}			
				record_list=record_list->next;

			}

			//for last element
			if(record_list->cType == T_INT)
			{
				if(strcmpi( record_list->record_string ,"NULL")==0)
				{

					//printf("check\n");
					char m = 0;
					fwrite(&m,1,1,fp);						
					int a = atoi(record_list->record_string);
					fwrite(&a ,4,1,fp);

				}

				else
				{
					//printf("check2");
					int a = atoi(record_list->record_string);
					fwrite(record_list,1,1,fp);
					fwrite(&a ,4,1,fp);
				}				
			}


			else
			{
				if((strcmpi( record_list->record_string ,"NULL")==0))
				{

					//printf("check3\n");
					record_list->col_len = 0;
					fwrite(record_list,record_list->max_length +1,1,fp);
				}

				else
				{
					//printf("check4\n");
					fwrite(record_list,record_list->max_length+1,1,fp);
				}
			}
		}

		else
		{
			printf("\n***********************************\n");
			printf("All ROWS DELETED. Table is empty now");
			printf("\n***********************************\n");
			
		}

	} //file error
	printf("New SIZE  %d,%d,%d,%d,%d\n",header->file_size,header->record_size,header->num_records,header->record_offset,header->file_header_flag);
	printf("\n************************\n");
	printf("%d Rows DELETED",o_size-size);
	printf("\n************************\n");

	fflush(fp);
	fclose(fp);
	}

	else
	{
		printf("\n************************\n");
		printf("NO ROWS TO DELETE!!");
		printf("\n************************\n");
    }

	return rc;

}

 int sem_backup_table(token_list  *t_list)
 {

	 int rc = 0;
	 printf("INSIDE BACKUP FUNCTION\n");

	 token_list *cur ;
	 cur = t_list;

	 FILE *fh1;
	 if(cur->tok_class == identifier)
	 {
		 //cur = cur->next;
		 if(cur->next->tok_value == EOC)
		 {

			 char imagename[MAX_IDENT_LEN];
			 strcpy(imagename,cur->tok_string);
			 printf("Backup image name:%s\n",cur->tok_string);

			 if( (_access(imagename, 0 )) != -1 )
			 {
				 printf("Error:Duplicate backup image name\n");
				 rc = DUPLICATE_BACKUP;
				 cur->tok_value= INVALID;
				 return rc;
				 
			 }

			 else
			 {
				 printf("Creating new Backup file\n");
				 char *buffer;
				 buffer = (char*)calloc(1,g_tpd_list->list_size);

				 fh1= fopen("dbfile.bin","rbc");

				 if(fh1 == NULL)
				 {
					 printf("File error\n");
					 rc = FILE_OPEN_ERROR;
	 
				 }
				 else
				 {
					 printf("Reading from dbfile\n");
					 fread(buffer,(g_tpd_list->list_size),1,fh1);
	 
				 }

				// printf("%s\n",buffer);
				 fflush(fh1);
				 fclose(fh1);

				 FILE *fb;

				 fb = fopen(imagename,"wbc");

				  if(fb == NULL)
				 {
					 printf("File error\n");
					 rc = FILE_OPEN_ERROR;
	 
				 }
				 else
				 {
					printf("Writing dbfile into Backup\n");
					fwrite(buffer, (g_tpd_list->list_size),1,fb);

					int num_tables = g_tpd_list->num_tables;
					tpd_entry *cur1 = &(g_tpd_list->tpd_start);
					int len;
					char tableName[MAX_IDENT_LEN];
					strcpy(tableName,cur1->table_name);
					strcat(tableName,".tab");

					if (num_tables == 0)
					{
						printf("\nThere are currently no tables defined\n");
					}
					else
					{
						table_file_header *header;
						header = (table_file_header*)calloc(1, sizeof(table_file_header));
						int len;
						FILE *ft ;
				
						while(num_tables != 0 )
						{
							printf("enter into while\n");
							printf("%s\n",tableName);
							ft = fopen(tableName,"rbc");

							if(ft == NULL)
							{
								rc = FILE_OPEN_ERROR;
				
							}

							else
							{

								fread(header,sizeof(table_file_header),1,ft);
								len = header->file_size;

								printf("Table size:%d\n",len);
								printf("Writing table size into backup\n");
								fwrite(&len,sizeof(int),1,fb);

								printf("Read table file\n");
								char *buffer2;
								buffer2 = (char*)calloc(1 , header->file_size);
								fseek(ft,0,SEEK_SET);
								fread(buffer2, (header->file_size)*sizeof(char) ,1 ,ft);						
								printf("Writing table into Backup\n");
								fwrite(buffer2,header->file_size,1,fb);
						
							}

							fflush(ft);
							fclose(ft);
										
							cur1 = (tpd_entry*)((char*)cur1 + cur1->tpd_size);
							strcpy(tableName,cur1->table_name);
							strcat(tableName,".tab");		
							num_tables--;
						}
							
					} //while
	 
				 }//file  for fb

				fflush(fb);
				fclose(fb);

			} //duplicate file
		} //eoc

		else
		{
			printf("Error:Backup statement not valid\n");
			rc = INVALID_BACKUP;
			cur->next->tok_value= INVALID;
			return rc;
        }

	 }//valid identifier

	 else
	 {
		 rc = INVALID_BACKUP;
		 cur->tok_value= INVALID;
		 printf("Error: Invalid Identifier\n");
		 return rc;
	 	 
	 }

	return rc;
 }

 int sem_restore_from_table( token_list *t_list)
 {
	 printf("enter sem_restore\n");
	 int rc = 0;
	 token_list *cur;
	 cur = t_list;
	 int rf_flag;

	 printf("%s\n",cur->tok_string);

	 if(cur->tok_class == identifier)
	 {
		 char file_name[MAX_IDENT_LEN];
		 strcpy(file_name,cur->tok_string);
		 printf("%s\n",file_name);

		 if( (_access(file_name, 0 )) == -1 )
		{
		  printf("Error: Transaction log does not contain the backup image name specified\n");
		  rc = INVALID_BACKUP;
		  cur->tok_value= INVALID;
		  return rc;				 
		}

	    else
	    {
			 cur= cur->next;

			 if(cur->tok_value == EOC)
			 {
				 printf("With RF\n");
				 printf("Parsing Successful!\n");
				 rf_flag = 1;
				 rc = restore(rf_flag,file_name);
		 		 
			 }
			 else
			 {
				if( (cur->tok_value == K_WITHOUT ) && (cur->next->tok_value == K_RF) )
				{
			
					//printf("ENTER without for\n");
					printf("Without RF\n");

					cur = cur->next->next;

					if(cur->tok_value != EOC)
					{
						printf("Error: Expecting EOC\n");
						rc = INVALID_RESTORE;
						cur->tok_value = INVALID;
						return rc;				
					}

					else
					{
						printf("Parsing Successful!\n");
						rf_flag = 0;
						rc = restore(rf_flag,file_name);
						//call restore function here
				
					}

				}
				else if( (cur->tok_value == K_WITHOUT ) && (cur->next->tok_value != K_RF) )
				{
					printf("Error: Expecting RF\n");
					rc = INVALID_RESTORE;
					cur->next->tok_value = INVALID;
					return rc;
		
				}
				else
				{
					printf("Error: Expecting WITHOUT or EOF\n");
					rc = INVALID_RESTORE;
					cur->tok_value = INVALID;
					return rc;			
				}
		 		 
			 }	 

		 } // backup does not exist
	 } //identifier
	 else
	 {
		 printf("Error:Invalid Identifier\n");
		 rc = INVALID_RESTORE;
		 cur->tok_value = INVALID;
		 return rc;
	 }
	 return rc;
 }
 
 int restore(int rf_flag, char *backup_file_name)
 {
	 int rc = 0;

	 if(rf_flag == 1)
	 {
		 /*--------------------------------------------------------BACKUP-------------------------------- */
		  printf("Just take backup without rollforward\n");
		// printf("Prune the log\n");
		 printf("First restore the file frm backup\n");

		 FILE *fb;

		 fb = fopen(backup_file_name,"rbc");
		 if( fb == NULL)
		 {
			 rc = FILE_OPEN_ERROR;
		 
		 
		 }
		 else
		 {
			struct _stat file_stat;
			_fstat(_fileno(fb), &file_stat);

			char table_name[MAX_IDENT_LEN];			
			tpd_list *t_list = NULL;
			t_list = (tpd_list*)calloc(1, file_stat.st_size  );
			fread(t_list,file_stat.st_size, 1,fb);
			fseek(fb,0,SEEK_SET);
			int db_size = t_list->list_size;
			int num_tables = t_list->num_tables;
			tpd_entry *cur1 = &(t_list->tpd_start);


			char *buffer = (char*)calloc(1,db_size);
			fread(buffer,db_size,1,fb);

			/* create dbfile.bin*/
			FILE *db_handle;

			db_handle = fopen("dbfile.bin","wbc");
			if(db_handle == NULL)
			{
				rc = FILE_OPEN_ERROR;
				return rc;
			}
			else
			{
				printf("Writing into file\n");
				fwrite(buffer,db_size,1,db_handle);		
				fclose(db_handle);
			    fflush(db_handle);
			}				 			

			 /*creating tab1.tab and rest of the files*/

			 int len;
			 char *buffer2;

			 printf("num of tables:%d",num_tables);
			 while(num_tables > 0)
			 {
				 printf("entering while\n");
				 FILE *tab_handle;

				 strcpy(table_name,cur1->table_name);
				 printf("%s",table_name);
				 //strcat(table_name,"New");
				 strcat(table_name,".tab");
				 

				 tab_handle = fopen(table_name,"wbc");
				 if(tab_handle == NULL)

				 {
					 rc = FILE_OPEN_ERROR;
					 printf("Error:Could not recover %s:\n", table_name);
					 //return rc;
				 }

				 else
				 {
					 printf("Success!\n");
					 fread(&len,sizeof(int),1,fb);
					 printf("len of tabl:%d\n",len);
					 buffer2 = (char*)calloc(1,len);
					 fread(buffer2,len,1,fb);
					 fwrite(buffer2,len,1,tab_handle);
					 free(buffer2);						
					 fclose(tab_handle);
					 fflush(tab_handle);
					 				 
				 }
				  printf("tpd size:%d",cur1->tpd_size);
				  cur1 = (tpd_entry*)((char*)cur1 + cur1->tpd_size);
				  num_tables--;
			 } //while

			  fclose(fb);
		      fflush(fb);
		} //file open error

//////////////////////////////////////////////BACKUP OVER////////////////////////////////////////////////////

//////////////////////////////////////ADD RF_START TAG//////////////////////////////////////////////////////

	
		FILE *fl;     
		fl = fopen("db.log","r");
		time_t t = time(0);
	    struct tm* lt = localtime(&t);
	    char time_str[15];
		int flag = 0;

		struct _stat file_stat;
		_fstat(_fileno(fl),&file_stat);

		char *command_till_backup;
		command_till_backup = (char*)calloc(1, file_stat.st_size);
		if(fl == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			int c;
			char *buffer1;
			buffer1 = (char*)calloc(1, (file_stat.st_size));
			//char timestamp[16];
			char *timestamp = (char*)calloc(1, 15);
		//	char *command_till_backup;
		//	command_till_backup = (char*)calloc(1, (sizeof(char))*1000 );
			
			char command[1000] ="backup to " ;		 
			strcat(command,backup_file_name);
			printf("%s\n",command);

			//int i=0;

			while ((c = fgetc(fl)) != EOF)
			{
				fseek(fl,-1,SEEK_CUR);
				fread(timestamp,15,1,fl);
				printf("%s\n",timestamp);
				fgetc(fl);
				
				int i =  0;
				while((c = fgetc(fl)) != '\"')
				{		
					buffer1[i]= (char)c;
					i++;
				}		

				printf("%s\n",buffer1);

				if(strcmpi(buffer1 ,command ) == 0)
				{
					printf("FOUND BACKUP COMMAND\n");
					strcat(command_till_backup,timestamp);
					strcat(command_till_backup,"\"");
					strcat(command_till_backup ,buffer1);
					strcat(command_till_backup,"\"\n");
					sprintf(time_str, "%04d%02d%02d%02d%02d%02d",lt->tm_year + 1900, lt->tm_mon + 1, lt->tm_mday,lt->tm_hour, lt->tm_min, lt->tm_sec);
					strcat(command_till_backup,time_str);
					strcat(command_till_backup," \"");
					//strcat(command_till_backup,"\"");
					strcat(command_till_backup,"RF_START");
					strcat(command_till_backup,"\"\n");

					flag = 1;							
				}
				else
				{
					printf(" DIDNt FIND BACKUP COMMAND\n");
					strcat(command_till_backup,timestamp);
					strcat(command_till_backup,"\"");
					strcat(command_till_backup ,buffer1);
					strcat(command_till_backup,"\"\n");

				}

				memset(buffer1,'\0',1000);
				memset(timestamp,'\0',15);
				c = fgetc(fl); //reads newline char
				//strcat(command_till_backup, (char)c);
			} //while

			free(buffer1);
			free(timestamp);
			
			printf("Before backup::%s",command_till_backup);

			fflush(fl);
			fclose(fl);
		}

		if(flag == 0)
		{
			printf("NO BACKUP TAG FOUND\n");
				
		}

		if(flag == 1)
		{
			/* Now write command_till_backup*/
			printf("PERFORMING RF\n");
			FILE *new_log;

			new_log = fopen("db.log","w");

			if(fl == NULL  )
			{
				rc = FILE_OPEN_ERROR;
		
			}
			else
			{
				fwrite(command_till_backup,strlen(command_till_backup),1,new_log);	
				fflush(new_log);
				fclose(new_log);
		
			}

		}
		free(command_till_backup);

		/*******************SET FLAG = ROLLFORWARD_PENDING************************************/
		 printf("Setting db flag to 1\n");
		 tpd_list *t_list;
		 t_list = (tpd_list*)calloc(1, sizeof(tpd_list));
		 FILE *db_handle;
		 db_handle=fopen("dbfile.bin","r+b");

		 if(db_handle== NULL)
		 {
			 rc = FILE_OPEN_ERROR;
		 
		 }
		 else
		 {
			 printf("HERE");
			 fread(t_list,sizeof(tpd_list),1,db_handle);
			 t_list->db_flags = ROLLFORWARD_PENDING;
			 fseek(db_handle,0,SEEK_SET);
			 fwrite(t_list,sizeof(tpd_list),1,db_handle);
			 fflush(db_handle);
			 fclose(db_handle);
		 }

	 }//with rf

	 else
	 {
		 printf("Just take backup without rollforward\n");
		// printf("Prune the log\n");
		 printf("First restore the file frm backup\n");

		 FILE *fb;

		 fb = fopen(backup_file_name,"rbc");

		 if( fb == NULL)
		 {
			 rc = FILE_OPEN_ERROR;
		 		 
		 }
		 else
		 {
			struct _stat file_stat;
			_fstat(_fileno(fb), &file_stat);

			char table_name[MAX_IDENT_LEN];			
			tpd_list *t_list = NULL;
			t_list = (tpd_list*)calloc(1, file_stat.st_size  );
			fread(t_list,file_stat.st_size, 1,fb);
			fseek(fb,0,SEEK_SET);
			int db_size = t_list->list_size;
			int num_tables = t_list->num_tables;
			tpd_entry *cur1 = &(t_list->tpd_start);


			char *buffer = (char*)calloc(1,db_size);
			fread(buffer,db_size,1,fb);

			/* create dbfile.bin*/
			FILE *db_handle;

			db_handle = fopen("dbfile.bin","wbc");
			if(db_handle == NULL)
			{
				rc = FILE_OPEN_ERROR;
				return rc;
			}
			else
			{
				printf("Writing into file\n");
				fwrite(buffer,db_size,1,db_handle);		
				fclose(db_handle);
			    fflush(db_handle);
			}				 			

			 /*creating tab1.tab and rest of the files*/

			 int len;
			 char *buffer2;

			 printf("num of tables:%d",num_tables);
			 while(num_tables > 0)
			 {
				 printf("entering while\n");
				 FILE *tab_handle;

				 strcpy(table_name,cur1->table_name);
				 printf("%s",table_name);
				 //strcat(table_name,"New");
				 strcat(table_name,".tab");
				 

				 tab_handle = fopen(table_name,"wbc");
				 if(tab_handle == NULL)

				 {
					 rc = FILE_OPEN_ERROR;
					 printf("Error:Could not recover %s:\n", table_name);
					 //return rc;
				 }

				 else
				 {
					 printf("Success!\n");
					 fread(&len,sizeof(int),1,fb);
					 printf("len of tabl:%d\n",len);
					 buffer2 = (char*)calloc(1,len);
					 fread(buffer2,len,1,fb);
					 fwrite(buffer2,len,1,tab_handle);
					 free(buffer2);						
					 fclose(tab_handle);
					 fflush(tab_handle);
					 				 
				 }
				  printf("tpd size:%d",cur1->tpd_size);
				  cur1 = (tpd_entry*)((char*)cur1 + cur1->tpd_size);
				  num_tables--;
			 } //while

			  fclose(fb);
		      fflush(fb);
		} //file open error
		
        /*------------------------------------------------LOG PRUNING-----------------------------------------------*/

		 /*GET BACKUP of db.log*/

		 //printf("XXX\n");
		 char *entire_file;
		
		 FILE *fl;
		 fl = fopen("db.log","r");
		 if(fl == NULL)
		 {
			 rc = FILE_OPEN_ERROR;
		 
		 }

		 else
		 {
			 struct _stat file_stat;
			_fstat(_fileno(fl), &file_stat);

			 entire_file = (char*)calloc(file_stat.st_size,1 );
			 fread(entire_file,file_stat.st_size,1,fl);
			 fclose(fl);
			 fflush(fl);
		 }

		 int j = 1;
    
		 char log_backup[50] ;
		 char log_num[15];
		 //strcat(log_backup,(char)j);
		 strcpy(log_backup,"db.log");
		 sprintf(log_num,"%d",j);
		 strcat(log_backup,log_num);
		 printf("%s",log_backup);

		 while((_access(log_backup, 0 )) != -1)
		 {
			 printf("file exists\n");
			 j++;
			 memset(log_backup,'\0',50);
			 strcpy(log_backup,"db.log");
			 sprintf(log_num,"%d",j);
			 strcat(log_backup,log_num);
		 }

		 FILE *log_handle;
		 printf("\nFILE NAME: %s\n",log_backup);

		 log_handle = fopen(log_backup,"w");

		 if(log_handle == NULL)
		 {
			 rc = FILE_OPEN_ERROR;
		 
		 }

		 else
		 {
			 fwrite(entire_file,strlen(entire_file),1,log_handle);
			 fflush(log_handle);
			 fclose(log_handle);
		 				 
		 }

		 free(entire_file);

/**********************backup of db.log*****************************/

		printf("LOG PRUNING\n");

	//	char *command_till_backup;
	//	command_till_backup = (char*)calloc(1, (sizeof(char))*1000 );

		char command[1000] ="backup to " ;		
		strcat(command,backup_file_name);
	    printf("%s\n",command);
		//FILE *fl;     
		fl = fopen("db.log","r");

		//struct _stat file_stat;
		//_fstat(_fileno(fl), &file_stat);


		char *command_till_backup = NULL;
		//command_till_backup = (char*)calloc(1, file_stat.st_size );

		if(fl == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			
		   // char *command_till_backup;
		  //  command_till_backup = (char*)calloc(1, file_stat.st_size );
			struct _stat file_stat;
		   _fstat(_fileno(fl), &file_stat);
			command_till_backup = (char*)calloc(1, file_stat.st_size );
			int c;
			char *buffer1;
			buffer1 = (char*)calloc(1, file_stat.st_size );
			//char timestamp[15];
			char *timestamp = (char*)calloc(1, 15);
			while ((c = fgetc(fl)) != EOF)
			{
				fseek(fl,-1,SEEK_CUR);
				fread(timestamp,15,1,fl);
				printf("%s\n",timestamp);
				fgetc(fl);
				int i =  0;
				while((c = fgetc(fl)) != '\"')
				{		
					buffer1[i]= (char)c;
					i++;
				}		

				printf("%s\n",buffer1);

				if(strcmpi(buffer1 ,command ) == 0)
				{
					printf("FOUND BACKUP COMMAND\n");
					strcat(command_till_backup,timestamp);
					strcat(command_till_backup,"\"");
					strcat(command_till_backup ,buffer1);
					strcat(command_till_backup,"\"\n");
					break;							
				}
				else
				{
					printf(" DIDNt FIND BACKUP COMMAND\n");
					strcat(command_till_backup,timestamp);
					strcat(command_till_backup,"\"");
					strcat(command_till_backup ,buffer1);
					strcat(command_till_backup,"\"\n");

				}

				memset(buffer1,'\0',1000);
				memset(timestamp,'\0',15);
				fgetc(fl); //reads newline char

			} //while

			printf("%s",command_till_backup);

			free(buffer1);
			free(timestamp);
			fflush(fl);
			fclose(fl);
		}

		/***************************************PRUNE THE db.log*******************************************************/

		fl = fopen("db.log","w");
		if(fl == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			fwrite(command_till_backup,strlen(command_till_backup),1,fl);
			fflush(fl);
			fclose(fl);
		}

		free(command_till_backup);

	 } //without rf
	return rc;
 }


 int sem_rollforward(token_list *t_list)
 {
	 int rc = 0;
	 printf("Enter SEM_ROLLFORWARD\n");
	 log = 0;

	 token_list *cur;
	 cur = t_list;

	 int time_flag;
 
	 if (cur->tok_value == EOC)
	 {
		 printf("Parsing Successful!!\n");
		 time_flag = 0;
		 rc = rollforward(time_flag,"");
	 
	 }
 
	 else
	 {
		 if (cur->tok_value != K_TO)
		 {
			rc = INVALID_ROLLFORWARD;
			cur ->tok_value =INVALID;
			return rc;		 
		 }

		 else
		 {
			 cur = cur->next;
			 if(cur->tok_class != constant)
			 {
				 rc = INVALID_ROLLFORWARD;
				 cur->tok_value = INVALID;
				 return rc;

			 }

			 else
			 {
				 char *timestamp;
				 //char timestamp[14];
				 timestamp = (char*)calloc( 14,1 );
				 strcpy(timestamp,cur->tok_string);
				 if(strlen(timestamp)  != 14)
				 {
					 printf("Error: TimeStamp not of correct format\n");
					 rc = INVALID_ROLLFORWARD;
					 cur->tok_value = INVALID;
					 return rc;
				 
				 }
				 else
				 {
					 cur =cur->next;
					 if (cur->tok_value == EOC)
					 {
						 printf("Parsing Successful\n");
						 time_flag = 1;
						 rc = rollforward(time_flag ,timestamp);
					 
					 }
					 else
					 {
						printf("Error: EOC not present\n");
						rc = INVALID_ROLLFORWARD;
						cur->tok_value = INVALID;
						return rc;					 
					 }								 
				 }			 
			 }		 
		 }	 
	 }
	 return rc;
 }

 int rollforward(int time_flag, char *rf_timestamp)
 {
	 int rc = 0;
     //printf("Enter ROLLFORWARD\n");

	// printf("Setting db flag to 0\n");
	 tpd_list *t_list;
	 t_list = (tpd_list*)calloc(1, sizeof(tpd_list));
	 FILE *db_handle;
	 db_handle=fopen("dbfile.bin","r+b");

	 if(db_handle== NULL)
	 {
		rc = FILE_OPEN_ERROR;
		 
	 }
	 else
	 {
		//printf("HERE");
		fread(t_list,sizeof(tpd_list),1,db_handle);

		if(t_list->db_flags == 0)
		{
			printf("Error: Database is not in ROLLFORWARD_PENDING state\n");
			rc = INVALID_ROLLFORWARD;
			return rc;
		
		}
		else
		{
			t_list->db_flags = 0;
			fseek(db_handle,0,SEEK_SET);
			fwrite(t_list,sizeof(tpd_list),1,db_handle);
			fflush(db_handle);
			fclose(db_handle);
		}
	 }
	 //////////////////backup db.log/////////////////////

	 
		 /*GET BACKUP of db.log*/

		 //printf("XXX\n");
		 char *entire_file;
		
		 FILE *fl;
		 fl = fopen("db.log","r");
		 if(fl == NULL)
		 {
			 rc = FILE_OPEN_ERROR;
		 
		 }

		 else
		 {
			 struct _stat file_stat;
			_fstat(_fileno(fl), &file_stat);

			 entire_file = (char*)calloc(file_stat.st_size,1 );
			 fread(entire_file,file_stat.st_size,1,fl);
			 fclose(fl);
			 fflush(fl);
		 }

	
/**********************backup of db.log*****************************/

	 ////////////////////backup//////////////////

	 char *pattern = "RF_START";
	 //FILE *fl;
	 fl = fopen("db.log","r");

	 struct _stat file_stat;
	_fstat(_fileno(fl),&file_stat);

	 int c;
	 char *buffer1;
	 buffer1 = (char*)calloc(1, file_stat.st_size );
	 char *timestamp = (char*)calloc(1, 15);


	 char *command_without_rf = (char*) calloc(1 , file_stat.st_size);

	 int flag =0;
	 if(fl == NULL)
	 {
		 rc = FILE_OPEN_ERROR;
		 return rc;	 
	 }

	 else
	 {
		 while ((c = fgetc(fl)) != EOF)
		{
			fseek(fl,-1,SEEK_CUR);
			fread(timestamp,15,1,fl);
			printf("%s\n",timestamp);
			fgetc(fl);
			int i =  0;
			while((c = fgetc(fl)) != '\"')
			{		
				buffer1[i]= (char)c;
				i++;
			}		

			printf("%s\n",buffer1);

			if(strcmpi(buffer1 ,pattern ) == 0)
			{
				printf("FOUND RF_START COMMAND\n");
				flag = 1;
				fgetc(fl);
				break;							
			}
			else
			{
				printf(" DIDNt FIND RF_START COMMAND\n");
				strcat(command_without_rf,timestamp);
				strcat(command_without_rf,"\"");
				strcat(command_without_rf ,buffer1);
				strcat(command_without_rf,"\"\n");

			}

			memset(buffer1,'\0',1000);
			memset(timestamp,'\0',15);
			fgetc(fl); //reads newline char

			} //while

		 free(buffer1);
		 free(timestamp);

		 if (flag == 1)
		 {		
			char *query = (char *)calloc(1, 1000*(sizeof(char)));			 
			char *new_timestamp = (char*)calloc(1, 15);		


			printf("timestmp:%s",rf_timestamp);
			if(strlen(rf_timestamp)==0)
			{
				printf("RF without TIMESTAMP\n");
				while ((c = fgetc(fl)) != EOF)
				{
					fseek(fl,-1,SEEK_CUR);
					fread(new_timestamp,15,1,fl);
					printf("%s\n",new_timestamp);
					fgetc(fl);
					int i =  0;
					while((c = fgetc(fl)) != '\"')
					{		
						query[i]= (char)c;
						i++;
					}		

					printf("%s\n",query);
					strcat(command_without_rf,new_timestamp);
					strcat(command_without_rf,"\"");
					strcat(command_without_rf ,query);
					strcat(command_without_rf,"\"\n");

                    /*
					char temp[9];
			        strcpy(temp,query);
					printf("temp:%s\n",temp);

					*/
					if(strstr(query,"backup to") == NULL)
					{
						//printf("null\n");
						rc = execute(query); //execute the buffer query
					}
					else
					{
						printf("cant process query\n");
					
					}
					memset(query,'\0',1000);
					memset(new_timestamp,'\0',15);
					fgetc(fl); //reads newline char
				}//while

			}

			else
			{
				printf("RF to TIMESTAMP\n");
				//int rf_time = atoi(rf_timestamp);
				//printf("%d",rf_time);
				//int time;

//////////////////////////////////////////prune the log/////////////////////////////////////
				int j = 1;
    
				 char log_backup[50] ;
				 char log_num[15];
				 //strcat(log_backup,(char)j);
				 strcpy(log_backup,"db.log");
				 sprintf(log_num,"%d",j);
				 strcat(log_backup,log_num);
				 printf("%s",log_backup);

				 while((_access(log_backup, 0 )) != -1)
				 {
					 printf("file exists\n");
					 j++;
					 memset(log_backup,'\0',50);
					 strcpy(log_backup,"db.log");
					 sprintf(log_num,"%d",j);
					 strcat(log_backup,log_num);
				 }

				 FILE *log_handle;
				 printf("\nFILE NAME: %s\n",log_backup);

				 log_handle = fopen(log_backup,"w");

				 if(log_handle == NULL)
				 {
					 rc = FILE_OPEN_ERROR;
		 
				 }

				 else
				 {
					 fwrite(entire_file,strlen(entire_file),1,log_handle);
					 fflush(log_handle);
					 fclose(log_handle);
		 				 
				 }

				 free(entire_file);

//////////////////////////////////////////////////////////////////////////////
				while((c = fgetc(fl)) != EOF)
				{
					fseek(fl,-1,SEEK_CUR);
					fread(new_timestamp,14,1,fl);
					fseek(fl,2,SEEK_CUR);

					//time = atoi(timestamp);

					if (strcmp(timestamp,rf_timestamp) <= 0)
					{
						int i =  0;
						while((c = fgetc(fl)) != '\"')
						{		
							query[i]= (char)c;
							i++;
						} 

						printf("%s\n",query);
						strcat(command_without_rf,new_timestamp);
						strcat(command_without_rf," \"");
						strcat(command_without_rf ,query);
						strcat(command_without_rf,"\"\n");
					
					}
					else
					{
						break;
					
					}
					if(strstr(query,"backup to") == NULL)
					{
						rc = execute(query);
					}
					else
					{
						printf("cant process query\n");
					
					}
					memset(query,'\0',1000);
					memset(new_timestamp,'\0',15);
					fgetc(fl);			
				} //while

			}//timestamp

			free(query);
			free(timestamp);

		 } // flag

		 else		 
		 {
			 printf("Error:DIDNT find RF_START TAG\n");
			 rc = INVALID_ROLLFORWARD;
			 return rc;		 		 
		 }

		 fflush(fl);
		 fclose(fl);
	 }

	 printf("STring :::%s\n",command_without_rf);

	 //////////////////////////////// Creating db.log without rf//////////////////////////
	 FILE *new_log_handle;
	 new_log_handle = fopen("db.log","w");
	 if(new_log_handle == NULL)
	 {
		 rc = FILE_OPEN_ERROR;
	 }
	 else
	 {
		 fwrite(command_without_rf,strlen(command_without_rf),1,new_log_handle);
		 fflush(new_log_handle);
		 fclose(new_log_handle);	 
	 }
	 free(command_without_rf);
	 return rc;
}

int execute(char *query)
{
	printf("Entering Execute querry\n");
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;
	rc = initialize_tpd_list();

	if (rc)
	{
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
	}
	else
	{
		rc = get_token(query, &tok_list);

		// Test code 
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
				tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}

		if (!rc)
		{
			rc = do_semantic(tok_list);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					(tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}
	}
	 return rc;
 }
