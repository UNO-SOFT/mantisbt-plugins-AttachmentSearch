<?php

if ( !defined( 'MANTIS_DIR' ) ) {
	define( 'MANTIS_DIR', dirname(__FILE__) . '/../../..' );
}
if ( !defined( 'MANTIS_CORE' ) ) {
	define( 'MANTIS_CORE', MANTIS_DIR . '/core' );
}

require_once( MANTIS_DIR . '/core.php' );
// require_once( config_get( 'class_path' ) . 'MantisPlugin.class.php' );
require_once( MANTIS_CORE . '/plugin_api.php' );

$P = plugin_get( );

require_once( MANTIS_CORE . '/bug_api.php' );

if ( access_get_project_level() < UPDATER ) {
    return;
}

compress_enable();
html_robots_noindex();

layout_page_header_begin( plugin_lang_get( 'attachment_search' ) );
layout_page_header_end();

layout_page_begin( __FILE__ );

$f_query = gpc_get_string( 'query', '' );
$f_limit = gpc_get_int( 'limit', 10 );
?>
<div>
<form id="query" method="post" action="<?php echo plugin_page( 'attachment_search' ); ?>">
	<div>
	<label for="query"><a href="https://www.postgresql.org/docs/12/textsearch-controls.html#TEXTSEARCH-PARSING-QUERIES">websearch_to_tsquery</a></label>
	<input type="textarea" id="query" name="query" width="80" height="3" value="<?php echo "$f_query"; ?>" />
	<label for="limit">limit</label>
	<input id="limit" name="limit" type="number" max="100" min="3" step="1" value="<?php echo "$f_limit"; ?>" />
	</div>
	<div>
	<input type="submit" value="<?php echo plugin_lang_get( 'search' ); ?>" />
	</div>
</form>
</div>
<?php
if( $f_query ) {
	// echo "<pre>f_query=$f_query</pre>\n";
	$t_rows = $P->do_query( $f_query, $f_limit );
	// echo "<pre>" . var_export( $t_rows, TRUE ) . "</pre\n";
	if( !$t_rows ) {
		echo "<pre>$t_rows</pre\n";
	} else {
		/*
SELECT A.*, ts_headline('hungarian', A.document, query) AS headline
  FROM (
SELECT A.bug_id AS bug_id, A.typ, A.id, ts_rank_cd(A.tsvec, query, 16) AS rank, 
       B.summary AS document
		*/
		echo "<table><thead><tr><th>ID</th><th>Tipus</th><th>Talalat</th></tr></thead>\n<tbody>";
		foreach( $t_rows as $t_row ) {
			$t_frag = '';
			switch( $t_row['typ'] ) {
				case 'F': $t_frag = '#f' . $t_row['id']; break;
				case 'N': $t_frag = '#c' . $t_row['id']; break;
			}
			echo "<tr><td><a target=\"_blank\" href=\"view.php?id=" . $t_row['bug_id'] . $t_frag . "\">" . $t_row['bug_id']  . "</td><td>" . $t_row['typ'] . "</td><td><p>" . $t_row['headline'] . "</p></td></tr>\n";
		}
		echo "</tbody></table>\n";
	}
}
layout_page_end();

