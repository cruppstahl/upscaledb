/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 */

package de.crupp.hamsterdb;

public class Const {
	
	// Error codes
	public final static int HAM_SUCCESS                  		= 	     0;
	public final static int HAM_INV_KEYSIZE              		= 	    -3;
	public final static int HAM_INV_PAGESIZE             		= 	    -4;
	public final static int HAM_OUT_OF_MEMORY            		= 	    -6;
	public final static int HAM_NOT_INITIALIZED          		= 	    -7;
	public final static int HAM_INV_PARAMETER            		= 	    -8;
	public final static int HAM_INV_FILE_HEADER          		=	    -9;
	public final static int HAM_INV_FILE_VERSION         		=	   -10;
	public final static int HAM_KEY_NOT_FOUND            		=	   -11;
	public final static int HAM_DUPLICATE_KEY            		=	   -12;
	public final static int HAM_INTEGRITY_VIOLATED       		=	   -13;
	public final static int HAM_INTERNAL_ERROR           		=	   -14;
	public final static int HAM_DB_READ_ONLY             		=	   -15;
	public final static int HAM_BLOB_NOT_FOUND           		=	   -16;
	public final static int HAM_PREFIX_REQUEST_FULLKEY   		=	   -17;
	public final static int HAM_IO_ERROR                 		=	   -18;
	public final static int HAM_CACHE_FULL               		=	   -19;
	public final static int HAM_NOT_IMPLEMENTED          		=	   -20;
	public final static int HAM_FILE_NOT_FOUND           		=	   -21;
	public final static int HAM_WOULD_BLOCK              		=	   -22;
	public final static int HAM_NOT_READY                		=	   -23;
	public final static int HAM_LIMITS_REACHED           		=	   -24;
	public final static int HAM_ACCESS_DENIED            		=	   -25;
	public final static int HAM_ALREADY_INITIALIZED      		=	   -27;
	public final static int HAM_CURSOR_IS_NIL           		=	  -100;
	public final static int HAM_DATABASE_NOT_FOUND      		=	  -200;
	public final static int HAM_DATABASE_ALREADY_EXISTS 		=	  -201;
	public final static int HAM_DATABASE_ALREADY_OPEN   		=	  -202;

	// Create/Open flags
	public final static int HAM_WRITE_THROUGH					= 	 0x001;
	public final static int HAM_READ_ONLY						= 	 0x004;
	public final static int HAM_USE_BTREE						= 	 0x010;
	public final static int HAM_DISABLE_VAR_KEYLEN				= 	 0x040;
	public final static int HAM_IN_MEMORY_DB					= 	 0x080;
	public final static int HAM_DISABLE_MMAP       				= 	 0x200;
	public final static int HAM_CACHE_STRICT       				= 	 0x400;
	public final static int HAM_DISABLE_FREELIST_FLUSH 			= 	 0x800;
	public final static int HAM_LOCK_EXCLUSIVE      			= 	0x1000;
	public final static int HAM_RECORD_NUMBER	 				= 	0x2000;
	public final static int HAM_ENABLE_DUPLICATES   			=   0x4000;
	
	// Extended parameters
	public final static int HAM_PARAM_CACHESIZE					= 	 0x100;
	public final static int HAM_PARAM_PAGESIZE					=    0x101;
	public final static int HAM_PARAM_KEYSIZE		       		= 	 0x102;
	public final static int HAM_PARAM_MAX_ENV_DATABASES	        =    0x103;
	
	// Database operations
	public final static int HAM_OVERWRITE						= 	  	 1;
	public final static int HAM_DUPLICATE						=	  	 2;
	public final static int HAM_DUPLICATE_INSERT_BEFORE 		= 		 4;
	public final static int HAM_DUPLICATE_INSERT_AFTER 			= 		 8;
	public final static int HAM_DUPLICATE_INSERT_FIRST 			= 		16;
	public final static int HAM_DUPLICATE_INSERT_LAST 			= 		32;
	public final static int HAM_AUTO_CLEANUP					= 		 1;
	
	// Cursor operations
	public final static int HAM_CURSOR_FIRST 					= 		 1;
	public final static int HAM_CURSOR_LAST 					= 		 2;
	public final static int HAM_CURSOR_NEXT 					= 		 4;
	public final static int HAM_CURSOR_PREVIOUS 				= 		 8;
	public final static int HAM_SKIP_DUPLICATES 				= 		16;
	public final static int HAM_ONLY_DUPLICATES 				= 		32;
	

}
