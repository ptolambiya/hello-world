create or replace 
PACKAGE pkg_ut_generator AS
  PROCEDURE generate_utplsql(p_package_name IN VARCHAR2);
END pkg_ut_generator;
/

create or replace 
PACKAGE BODY pkg_ut_generator AS
  PROCEDURE generate_utplsql(p_package_name IN VARCHAR2) IS
    l_test_package VARCHAR2(32767);
    l_proc_source CLOB;    
    l_column_list CLOB;    
    l_table_name varchar2(50);
  BEGIN
    -- Initialize the test package header
    l_test_package := 'CREATE OR REPLACE PACKAGE UT_' || p_package_name || ' AS ';
    l_test_package := l_test_package || '  
    -- Test procedures ';
    
    FOR rec IN (
      SELECT procedure_name name, object_type type
      FROM user_procedures
      WHERE object_name = UPPER(p_package_name)
      AND procedure_name IS NOT NULL
    ) LOOP
      l_test_package := l_test_package || 
      '    
        --%test
        --%displayname(' || p_package_name || ':test ' || replace(LOWER(rec.name),'_',' ') || ')
        PROCEDURE test_' || LOWER(rec.name) || ';';
    END LOOP;
    
    l_test_package := l_test_package || '
END UT_' || p_package_name || ';
/

';
    -- Initialize the test package body
    l_test_package := l_test_package || 'CREATE OR REPLACE PACKAGE BODY UT_' || p_package_name || ' AS 
    -- Prepare Data for Test
    ';
    
    FOR rec IN (
      SELECT procedure_name name, object_type type
      FROM user_procedures
      WHERE object_name = UPPER(p_package_name)
      AND procedure_name IS NOT NULL
    ) LOOP
      -- Extract source code of the procedure/function
      BEGIN
with
first_line as
(select line
   from user_source
  where instr(lower(text),'procedure '||lower(rec.name)) > 0
    and name = UPPER(p_package_name)
    and type = 'PACKAGE BODY'
),
last_line as
(select nvl(min(line),99999)-1 line
   from user_source
  where (lower(text) like '%procedure%' or lower(text) like '%function%')
    and name = UPPER(p_package_name)
    and type = 'PACKAGE BODY'
    and line > (select line from user_source
  where (lower(text) like '%procedure%'||lower(rec.name)||'%' or lower(text) like '%function%'||lower(rec.name)||'%')
    and name = UPPER(p_package_name)
    and type = 'PACKAGE BODY')
)
select replace(LISTAGG(text,' ') WITHIN GROUP (ORDER BY line),chr(10),'') INTO l_proc_source
  from user_source
 where name = UPPER(p_package_name) and type = 'PACKAGE BODY' and line between (select line from first_line)
                and (select line from last_line);
      
        -- Find tables used in the procedure/function
        FOR tbl_rec IN (
          SELECT table_name FROM user_tables
          WHERE EXISTS (
            SELECT 1 FROM dual
            WHERE REGEXP_LIKE(l_proc_source, ' ' || table_name || ' ', 'i')
          )
        ) LOOP
        -- Construct insert statement dynamically
        l_test_package := l_test_package || 'INSERT INTO ' || tbl_rec.table_name || ' (';
        l_column_list := '';
        l_table_name := tbl_rec.table_name;
        FOR col_rec IN (
          SELECT column_name
          FROM user_tab_columns
          WHERE table_name = tbl_rec.table_name
          ORDER BY column_id
        ) LOOP
        l_column_list := l_column_list || col_rec.column_name || ', ';
          l_test_package := l_test_package || col_rec.column_name || ', ';
        END LOOP;
        
        l_test_package := RTRIM(l_test_package, ', ') || ') VALUES (';
        
        FOR col_rec IN (
          SELECT column_name, data_type
          FROM all_tab_columns
          WHERE table_name = tbl_rec.table_name
          ORDER BY column_id
        ) LOOP
        
          l_test_package := l_test_package || 
            CASE 
              WHEN col_rec.data_type LIKE 'VARCHAR%' THEN '''test_value'''
              WHEN col_rec.data_type LIKE 'NUMBER%' THEN '123'
              WHEN col_rec.data_type LIKE 'DATE%' THEN 'SYSDATE'
              ELSE 'NULL'
            END || ', ';
        END LOOP;
        
        l_test_package := RTRIM(l_test_package, ', ') || ');
    COMMIT;
';
      END LOOP;
      END;
      -- Act & Assert section
      l_test_package := l_test_package || '    -- Act: Call the procedure/function 
    ' || CASE 
          WHEN rec.type = 'FUNCTION' THEN 'l_result := ' || p_package_name || '.' || rec.name || ';
' 
          ELSE p_package_name || '.' || rec.name || ';
' 
        END || '
    -- Assert: Match expected and actual cursors ';
    l_test_package := l_test_package || chr(10) ||'    OPEN l_actual for ' || RTRIM(l_column_list, ', ') || ' from '||l_table_name|| ';'||chr(10) ||
    '    OPEN l_expected FOR SELECT ';
      
      FOR col_rec IN (
        SELECT column_name, data_type
        FROM user_tab_columns
        WHERE table_name IN (
          SELECT table_name FROM user_tables
          WHERE EXISTS (
            SELECT 1 FROM dual
            WHERE REGEXP_LIKE(l_proc_source, ' ' || table_name || ' ', 'i')
          )
        )
        ORDER BY column_id
      ) LOOP
        l_test_package := l_test_package ||
          CASE 
            WHEN col_rec.data_type LIKE 'VARCHAR%' THEN '''test_value'''
            WHEN col_rec.data_type LIKE 'NUMBER%' THEN '123'
            WHEN col_rec.data_type LIKE 'DATE%' THEN 'SYSDATE'
            ELSE 'NULL'
          END || ' AS ' || col_rec.column_name || ', ';
      END LOOP;
      
      l_test_package := RTRIM(l_test_package, ', ') || ' FROM dual;
      
    ut.expect(l_actual).to_equal(l_expected);

  END test_' || LOWER(rec.name) || ';
';
    END LOOP;
    
    l_test_package := l_test_package || 'END UT_' || p_package_name || ';
/
';
    
    -- Output the generated package
    DBMS_OUTPUT.PUT_LINE(l_test_package);
  END generate_utplsql;
END pkg_ut_generator;
/
