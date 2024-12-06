<?php
# :vim set noet:

if ( !defined( 'MANTIS_DIR' ) ) {
	define( 'MANTIS_DIR', dirname(__FILE__) . '/../..' );
}
if ( !defined( 'MANTIS_CORE' ) ) {
	define( 'MANTIS_CORE', MANTIS_DIR . '/core' );
}

require_once( MANTIS_DIR . '/core.php' );
require_once( config_get( 'class_path' ) . 'MantisPlugin.class.php' );
// require_once( dirname(__FILE__).'/core/sla_api.php' );

require_api( 'install_helper_functions_api.php' );
require_api( 'authentication_api.php');
require_api( 'string_api.php');

require_api( 'http_api.php' );

class AttachmentSearchPlugin extends MantisPlugin {

	function register() {
		$this->name = 'AttachmentSearch';	# Proper name of plugin
		$this->description = 'Search attachments and full text';	# Short description of the plugin
		$this->page = '';		   # Default plugin page

		$this->version = '0.0.1';	 # Plugin version string
		$this->requires = array(	# Plugin dependencies, array of basename => version pairs
			'MantisCore' => '2.0.0'
		);

		$this->author = 'Tamás Gulácsi';		 # Author/team name
		$this->contact = 'T.Gulacsi@unosoft.hu';		# Author/team e-mail address
		$this->url = 'http://www.unosoft.hu';			# Support webpage
	}

	function config() { return array(); }

	function hooks(): array {
		return array(
			'EVENT_MENU_MAIN_FRONT'    => 'sidebar',
		);
	}

	function sidebar( $p_event ): array {
		if ( access_get_project_level() < UPDATER ) {
			return array( );
		}
		return array( array( 
			'url' => plugin_page( 'attachment_search' ),
			'title' => 'attachment_search',
			'icon' => 'file-text-o',
		) );
	}

	function init( ) {
		foreach( array(
"
CREATE MATERIALIZED VIEW IF NOT EXISTS mw_tsvec AS 
  SELECT id AS bug_id, 'Bs' AS typ, id, to_tsvector('hungarian', summary) AS tsvec 
    FROM mantis_bug_table
  UNION ALL
  SELECT B.id AS bug_id, 'Td' AS typ, A.id, to_tsvector('hungarian', A.description) AS tsvec
    FROM mantis_bug_text_table A 
         INNER JOIN mantis_bug_table B ON B.bug_text_id = A.id
  UNION ALL
  SELECT B.id AS bug_id, 'Ta' AS typ, A.id, to_tsvector('hungarian', A.additional_information) AS tsvec
    FROM mantis_bug_text_table A 
         INNER JOIN mantis_bug_table B ON B.bug_text_id = A.id
  UNION ALL
  SELECT B.id AS bug_id, 'Ts' AS typ, A.id, to_tsvector('hungarian', A.steps_to_reproduce) AS tsvec
    FROM mantis_bug_text_table A 
         INNER JOIN mantis_bug_table B ON B.bug_text_id = A.id
  UNION ALL
  SELECT B.bug_id, 'N' AS typ, A.id, to_tsvector('hungarian', A.note) AS tsvec
    FROM mantis_bugnote_text_table A
         INNER JOIN mantis_bugnote_table B ON B.bugnote_text_id = A.id 
",
			"GRANT SELECT ON mantis_plugin_attachment_search_table TO public",
			"GRANT SELECT ON mw_tsvec TO public",
		) as $t_qry ) {
			$t_result = db_query( $t_qry );
			if( !$t_result ) {
				db_error( $t_qry );
			}
		}
  	}

	function do_query( $p_query, $p_limit = 10 ) {
		//$this->init();
		
		$p_limit = (int)($p_limit);
		if( !$p_limit || $p_limit < 1 ) {
			$p_limit = 10;
		}
		$t_qry = "
WITH query AS (SELECT websearch_to_tsquery('hungarian', " . db_param() . ") AS query)
SELECT A.*, ts_headline('hungarian', A.document, query) AS headline
  FROM (
SELECT A.bug_id AS bug_id, A.typ, A.id, ts_rank_cd(A.tsvec, query, 16) AS rank, 
       B.summary AS document
  FROM mw_tsvec A INNER JOIN mantis_bug_table B ON B.id = A.id, query
  WHERE A.tsvec @@ query AND A.typ = 'Bs'
UNION ALL
SELECT A.bug_id, A.typ, A.id, ts_rank_cd(A.tsvec, query, 16) AS rank, 
       B.description AS document
  FROM mw_tsvec A INNER JOIN mantis_bug_text_table B ON B.id = A.id, query
  WHERE A.tsvec @@ query AND A.typ = 'Td'
UNION ALL
SELECT A.bug_id, A.typ, A.id, ts_rank_cd(A.tsvec, query, 16) AS rank, 
       B.additional_information AS document
  FROM mw_tsvec A INNER JOIN mantis_bug_text_table B ON B.id = A.id, query
  WHERE A.tsvec @@ query AND A.typ = 'Ta'
UNION ALL
SELECT A.bug_id, A.typ, A.id, ts_rank_cd(A.tsvec, query, 16) AS rank, 
       B.steps_to_reproduce AS document
  FROM mw_tsvec A INNER JOIN mantis_bug_text_table B ON B.id = A.id, query
  WHERE A.tsvec @@ query AND A.typ = 'Ts'
UNION ALL
SELECT A.bug_id, A.typ, A.id, ts_rank_cd(A.tsvec, query, 16) AS rank, 
       B.note AS document
  FROM mw_tsvec A INNER JOIN mantis_bugnote_text_table B ON B.id = A.id, query
  WHERE A.tsvec @@ query AND A.typ = 'N'
UNION ALL
SELECT B.bug_id, 'F' AS typ, A.file_id AS id, ts_rank_cd(A.tsvec, query, 16) AS rank, 
       A.content AS document
  FROM mantis_plugin_attachment_search_table A 
       INNER JOIN mantis_bug_file_table B ON B.id = A.file_id, query
  WHERE A.tsvec @@ query 
  ORDER BY rank DESC 
  LIMIT " . $p_limit . ") A, query";

		$t_result = db_query( $t_qry, array( $p_query ) );
		if( !$t_result ) {
			return db_error( $t_qry );
		}
		$t_rows = array();
		while( TRUE ) {
			$t_row = db_fetch_array( $t_result );
			if( !$t_row ) {
				break;
			}
			$t_rows[] = $t_row;
		}
		return $t_rows;
	}
}

// vim: set noet:
