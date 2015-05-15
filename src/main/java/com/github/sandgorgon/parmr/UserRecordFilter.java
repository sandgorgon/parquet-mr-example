/*
 * Copyright (C)2015 Micron Technology Inc. All Rights Reserved.
 * Unpublished - all rights reserved under the copyright laws of the  United States.
 * USE OF A COPYRIGHT NOTICE IS PRECAUTIONARY ONLY AND  DOES NOT IMPLY PUBLICATION OR DISCLOSURE.
 * 
 * THIS SOFTWARE CONTAINS CONFIDENTIAL INFORMATION AND TRADE SECRETS OF MICRON TECHNOLOGY, INC. USE, DISCLOSURE,
 * OR REPRODUCTION IS PROHIBITED WITHOUT THE PRIOR EXPRESS WRITTEN PERMISSION OF MICRON TECHNOLOGY, INC.
 */
package com.github.sandgorgon.parmr;

import parquet.column.ColumnReader;
import static parquet.filter.ColumnPredicates.equalTo;
import static parquet.filter.ColumnRecordFilter.column;
import static parquet.filter.NotRecordFilter.not;
import parquet.filter.RecordFilter;
import parquet.filter.UnboundRecordFilter;

/**
 *
 * @author rdevera
 */
public class UserRecordFilter implements UnboundRecordFilter {
    private final UnboundRecordFilter myFilter;
    
    public UserRecordFilter() {
        myFilter = not(column("favorite_number", equalTo(2)));
    }
    
    @Override
    public RecordFilter bind(Iterable<ColumnReader> itrbl) {
        return myFilter.bind(itrbl);
    }
    
}
