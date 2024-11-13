/// <p>This class overrides the standard Foreign Data Wrapper for JDBC connections with a 
/// small extension to load the <a href="https://duckdb.org/docs/extensions/iceberg.html">DuckDB
/// Iceberg extension</a> after connecting. To use this override, change the <pre>FDWNAME</pre>
/// class parameter in your Foreign Server's class to <pre>bdb.sql.fdw.DuckDB</pre>.</p>
/// <p>This class is <b>NOT AN OFFICIAL PART OF INTERSYSTEMS IRIS</b> and may not be compatible
/// with future versions. If you find this doesn't work, please raise an issue in the GitHub repository
/// you retrieved it from.</p>
/// <p>A more generic, supported way to override Foreign Data Wrapper classes will be provided in the future.</p>
Class bdb.sql.fdw.DuckDB Extends %SQL.FDW.XDBC
{

/// Overridden method
Method Prepare(server As %String, ftClassName As %String, query As %String) As %Status
{
	set tSC = $$$OK
	try {
		set requestObject = ..GetRequestObject(server)
		set ..Connection = ##class(%XDBC.Gateway.Connection).GetConnection(requestObject) // only JDBC for now so Connection is type %XDBC.Gateway.JDBC.Connection

        // DuckDB special!!
        do ..Connection.CreateStatement().Execute("LOAD Iceberg")

		set ..Statement = ..Connection.PrepareStatement(query, ##class(%XDBC.Gateway.JDBC.Connection).#TYPESCROLLFORWARDONLY, ##class(%XDBC.Gateway.JDBC.Connection).#CONCURREADONLY) // only JDBC for now so Statement is type %XDBC.Gateway.JDBC.Statement
		// Need to get the metadata during the prepare for THROUGH statements
		set ..XDBCMetadata = ..Statement.GetResultSetMetaData()
		set ..%ResultColumnCount = ..XDBCMetadata.GetColumnCount()
		// Now that we divide into Prepare() and Execute(), stash the query text in case we need to re-prepare due to an error in the execute
		set ..Query = query 
    } catch ex {
        if ex.Code = $$$ERRORCODE($$$GatewayException) {
			set ex.Data = "Remote database reported error: "_ex.Data
		}
		set tSC = ex.AsStatus()
		do ..SetSQLCODEFromStatus(tSC, ex.Data)
    }
	
	RETURN tSC
}

}
