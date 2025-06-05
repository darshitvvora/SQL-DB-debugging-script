WITH get_schema_names AS (
	SELECT schema_name
	  FROM information_schema.schemata 
	-- WHERE schema_owner <> 'postgres'
) SELECT DISTINCT 
         current_database(), 
         schemaname  as schema_name, 
         tablename   as table_name, 
         total_bytes as object_total_size_bytes, 
         pg_size_pretty(total_bytes) as object_total_size,
         (total_bytes-index_bytes-COALESCE(toast_bytes,0)) AS table_size_bytes,
         pg_size_pretty((total_bytes-index_bytes-COALESCE(toast_bytes,0))) AS table_size,
  	     ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) AS tbloat,
  	     CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END AS wastedbytes,
  	     pg_size_pretty(CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END) as wasted_size,
  	     sml.relpages::FLOAT, otta, seq_scan, idx_scan, rows_ins, rows_upd, rows_del, live_rows, dead_rows, last_vacuum, last_autovacuum
    FROM (
          SELECT rs.schemaname, 
                 rs.tablename, 
         		 cc.reltuples, 
         		 cc.relpages, 
         		 bs, 
    	   		 pg_total_relation_size(cc.oid) AS total_bytes,
         		 pg_indexes_size(cc.oid) AS index_bytes,
         		 pg_total_relation_size(cc.reltoastrelid) AS toast_bytes,
         		 CEIL((cc.reltuples*((datahdr+ma-(CASE 
         											WHEN datahdr%ma=0 THEN ma 
         											ELSE datahdr%ma
         										  END))+nullhdr2+4))/(bs-20::FLOAT)) AS otta,

         			 ptu.seq_scan as seq_scan, ptu.idx_scan as idx_scan, ptu.n_tup_ins as rows_ins, ptu.n_tup_upd as rows_upd, ptu.n_tup_del as rows_del, ptu.n_live_tup as live_rows, ptu.n_dead_tup as dead_rows, ptu.last_vacuum as last_vacuum, ptu.last_autovacuum as last_autovacuum
  		    FROM (  SELECT ma,
				    	   bs,
				    	   schemaname,
				    	   tablename,
				      	   (datawidth+(hdr+ma-(CASE WHEN hdr%ma=0 THEN ma ELSE hdr%ma END)))::NUMERIC AS datahdr,
				           (maxfracsum*(nullhdr+ma-(CASE WHEN nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
    				  FROM (SELECT schemaname, 
					      		   tablename, 
					      		   hdr, 
					      		   ma, 
					      		   bs,
					               SUM((1-null_frac)*avg_width) AS datawidth,
					               MAX(null_frac) AS maxfracsum,
					               hdr+(SELECT 1+COUNT(*)/8
					          		     FROM pg_stats s2
					          		    WHERE null_frac<>0 
					          		      AND s2.schemaname = s.schemaname 
					          		      AND s2.tablename = s.tablename) AS nullhdr
      					   FROM pg_stats s, (SELECT
									           (SELECT current_setting('block_size')::NUMERIC) AS bs,
									          		   CASE WHEN SUBSTRING(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
									          		   CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma
									              FROM (SELECT version() AS v) AS foo
									      ) AS constants
						GROUP BY 1,2,3,4,5
					) AS foo
			) AS rs
  		JOIN pg_class cc ON cc.relname = rs.tablename
  		JOIN pg_namespace nn ON cc.relnamespace = nn.oid 
  							AND nn.nspname = rs.schemaname 
  							AND nn.nspname <> 'information_schema'
   LEFT JOIN pg_index i ON indrelid = cc.oid
   LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
   JOIN pg_stat_user_tables ptu on cc.oid=ptu.relid
) AS sml 
INNER JOIN get_schema_names gsn ON gsn.schema_name = sml.schemaname
WHERE CASE 
		WHEN relpages < otta THEN 0 
		ELSE bs*(sml.relpages-otta)::BIGINT 
	  END > 0 
ORDER BY wastedbytes DESC;