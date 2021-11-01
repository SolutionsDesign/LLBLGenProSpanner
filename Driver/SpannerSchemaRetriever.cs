////////////////////////////////////////////////////////////////////////////////////////////
// Implementation of the Google Cloud Spanner DBDriver for the LLBLGen Pro system.
// (c) 2002-2019 Solutions Design, all rights reserved.
// http://www.llblgen.com/
// 
// THIS IS NOT OPEN SOURCE SOFTWARE OF ANY KIND. 
// 
// Designed and developed by Frans Bouma.
///////////////////////////////////////////////////////////////////////////////////////////
using System;
using System.Linq;
using System.Data;
using System.Collections.Generic;
using System.Text;
using SD.LLBLGen.Pro.DBDriverCore;
using System.Data.Common;
using SD.Tools.BCLExtensions.DataRelated;
using SD.Tools.BCLExtensions.CollectionsRelated;

namespace SD.LLBLGen.Pro.DBDrivers.Spanner
{
    /// <summary>
	/// Google Cloud Spanner specific implementation of DBSchemaRetriever
	/// </summary>
	public class SpannerSchemaRetriever : DBSchemaRetriever
	{

		/// <summary>
		/// CTor
		/// </summary>
		/// <param name="catalogRetriever">The catalog retriever to use</param>
		public SpannerSchemaRetriever(DBCatalogRetriever catalogRetriever)
			: base(catalogRetriever)
		{
		}


		/// <summary>
		/// Retrieves the table- and field meta data for the tables which names are in the passed in elementNames and which are in the schema specified.
		/// </summary>
		/// <param name="schemaToFill">The schema to fill.</param>
		/// <param name="elementNames">The element names.</param>
		/// <remarks>Implementers should add DBTable instances with the DBTableField instances to the DBSchema instance specified.
		/// Default implementation is a no-op</remarks>
		protected override void RetrieveTableAndFieldMetaData(DBSchema schemaToFill, IEnumerable<DBElementName> elementNames)
		{
			#region Descriptions of queries used.
			// per table, get fields, populate field structure for table.
			// SELECT C.TABLE_NAME, C.COLUMN_NAME, C.ORDINAL_POSITION, C.IS_NULLABLE, C.SPANNER_TYPE, CO.OPTION_NAME, CO.OPTION_TYPE, CO.OPTION_VALUE
			// FROM INFORMATION_SCHEMA.COLUMNS C LEFT JOIN INFORMATION_SCHEMA.COLUMN_OPTIONS CO ON C.TABLE_NAME=CO.TABLE_NAME AND C.COLUMN_NAME=CO.COLUMN_NAME
			// WHERE C.TABLE_SCHEMA='' AND C.TABLE_NAME=@sTableName
			// ORDER BY C.TABLE_NAME ASC, C.ORDINAL_POSITION
			//
			// ----------------------------------------------------------------
			// For the PK retrieval, this query is used:
			// SELECT I.TABLE_NAME, I.INDEX_NAME, I.PARENT_TABLE_NAME, IC.COLUMN_NAME, IC.ORDINAL_POSITION
			// FROM INFORMATION_SCHEMA.INDEXES I INNER JOIN INFORMATION_SCHEMA.INDEX_COLUMNS IC ON I.TABLE_SCHEMA=IC.TABLE_SCHEMA AND I.TABLE_NAME=IC.TABLE_NAME AND I.INDEX_NAME=IC.INDEX_NAME
			// WHERE I.TABLE_SCHEMA='' AND I.TABLE_NAME = @sTableName
			// ORDER BY I.TABLE_NAME ASC, I.INDEX_NAME ASC, IC.ORDINAL_POSITION ASC"
			#endregion
			
			string fieldQuery = "SELECT C.TABLE_NAME, C.COLUMN_NAME, C.ORDINAL_POSITION, C.IS_NULLABLE, C.SPANNER_TYPE, CO.OPTION_NAME, CO.OPTION_TYPE, CO.OPTION_VALUE FROM INFORMATION_SCHEMA.COLUMNS C LEFT JOIN INFORMATION_SCHEMA.COLUMN_OPTIONS CO ON C.TABLE_NAME=CO.TABLE_NAME AND C.COLUMN_NAME=CO.COLUMN_NAME WHERE C.TABLE_SCHEMA='' AND C.TABLE_NAME=@sTableName ORDER BY C.TABLE_NAME ASC, C.ORDINAL_POSITION";
			DbConnection connection = this.DriverToUse.CreateConnection();
			DbCommand fieldRetrievalCmd = this.DriverToUse.CreateCommand(connection, fieldQuery);
			DbParameter tableNameParameter = this.DriverToUse.CreateParameter(fieldRetrievalCmd, "@sTableName", DbType.String, 250);
			DbDataAdapter fieldRetrievalAdapter = this.DriverToUse.CreateDataAdapter(fieldRetrievalCmd);
		
			string pkRetrievalQuery = "SELECT I.TABLE_NAME, I.INDEX_NAME, I.PARENT_TABLE_NAME, IC.COLUMN_NAME, IC.ORDINAL_POSITION FROM INFORMATION_SCHEMA.INDEXES I INNER JOIN INFORMATION_SCHEMA.INDEX_COLUMNS IC ON I.TABLE_SCHEMA=IC.TABLE_SCHEMA AND I.TABLE_NAME=IC.TABLE_NAME AND I.INDEX_NAME=IC.INDEX_NAME WHERE I.TABLE_SCHEMA='' AND I.TABLE_NAME=@sTableName ORDER BY I.TABLE_NAME ASC, I.INDEX_NAME ASC, IC.ORDINAL_POSITION ASC";
			DbCommand pkRetrievalCmd = this.DriverToUse.CreateCommand(connection, pkRetrievalQuery);
			DbParameter pkRetrievalTableNameParam = this.DriverToUse.CreateParameter(pkRetrievalCmd, "@sTableName", DbType.String, 250);
			DbDataAdapter pkRetrievalAdapter = this.DriverToUse.CreateDataAdapter(pkRetrievalCmd);
			
			DataTable fieldsInTable = new DataTable();
			DataTable pkFieldsInTable = new DataTable();
			List<DBTable> tablesToRemove = new List<DBTable>();
			try
			{
				connection.Open();
				fieldRetrievalCmd.Prepare();
				pkRetrievalCmd.Prepare();
			
				// walk all tables in list of elementnames and pull information for this table out of the db.
				foreach(DBElementName tableName in elementNames)
				{
					if(tableName==null)
					{
						continue;
					}
					DBTable currentTable = new DBTable(schemaToFill, tableName);
					schemaToFill.Tables.Add(currentTable);
					tableNameParameter.Value = tableName.Name;
			
					// get the fields. 
					fieldsInTable.Clear();
					this.DriverToUse.ExecuteWithActiveRecoveryStrategy(() => fieldRetrievalAdapter.Fill(fieldsInTable));
			
					try
					{
						var fields = from row in fieldsInTable.AsEnumerable()
									 let typeDefinition = CreateTypeDefinition(row)
									 select new DBTableField(row.Value<string>("COLUMN_NAME"), typeDefinition)
									 {
										 OrdinalPosition =row.Value<int>("ORDINAL_POSITION"),
										 IsNullable = (row.Value<string>("IS_NULLABLE") == "YES"),
										 // set iscomputed to true if the DB has to make sure the timestamp value is set. This will make the entity field become readonly.
										 DefaultValue = (typeDefinition.DBType == (int)SpannerDbTypes.Timestamp && row.Value<string>("OPTION_NAME")=="allow_commit_timestamp" && 
														row.Value<string>("OPTION_VALUE")=="TRUE") ? "allow_commit_timestamp" : string.Empty,
										 ParentTable = currentTable
									 };
						currentTable.Fields.AddRange(fields);
			
						// get Primary Key fields for this table
						pkRetrievalTableNameParam.Value = tableName.Name;
						pkFieldsInTable.Clear();
						this.DriverToUse.ExecuteWithActiveRecoveryStrategy(() => pkRetrievalAdapter.Fill(pkFieldsInTable));
			
						foreach(DataRow row in pkFieldsInTable.AsEnumerable())
						{
							DBTableField primaryKeyField = currentTable.FindFieldByName(row.Value<string>("COLUMN_NAME"));
							if(primaryKeyField != null)
							{
								primaryKeyField.IsPrimaryKey = true;
								currentTable.PkConstraintName = row.Value<string>("INDEX_NAME") ?? string.Empty;
							}
						}
					}
					catch(ApplicationException ex)
					{
						// non fatal error, remove the table, proceed 
						schemaToFill.LogError(ex, "Table '" + currentTable.Name + "' removed from list due to an internal exception in Field population: " + ex.Message, "SpannerSchemaRetriever::RetrieveTableAndFieldMetaData");
						tablesToRemove.Add(currentTable);
					}
					catch(InvalidCastException ex)
					{
						// non fatal error, remove the table, proceed 
						schemaToFill.LogError(ex, "Table '" + currentTable.Name + "' removed from list due to cast exception in Field population.", "SpannerSchemaRetriever::RetrieveTableAndFieldMetaData");
						tablesToRemove.Add(currentTable);
					}
				}
			}
			finally
			{
				connection.SafeClose(true);
			}
			
			foreach(DBTable toRemove in tablesToRemove)
			{
				schemaToFill.Tables.Remove(toRemove);
			}
		}


		/// <summary>
		/// Creates the type definition from the data passed in
		/// </summary>
		/// <param name="row">The row with meta-data to convert to a TypeDefinition.</param>
		/// <returns>
		/// a DBTypeDefinition filled with the available information in the row.
		/// </returns>
		private DBTypeDefinition CreateTypeDefinition(DataRow row)
		{
			DBTypeDefinition toReturn = new DBTypeDefinition();
			string datatype = (row.Value<string>("SPANNER_TYPE") ?? string.Empty).ToLowerInvariant();

			int maxLength = 0;
			// string and bytes have the length specified in the type. E.g. STRING(50) or STRING(MAX). Same for BYTES(50) or BYTES(MAX)
			if(datatype.StartsWith("string"))
			{
				maxLength = DetermineMaxLength(datatype.Replace("string", ""));
				datatype = "string";
			}
			if(datatype.StartsWith("bytes"))
			{
				maxLength = DetermineMaxLength(datatype.Replace("bytes", ""));
				datatype = "bytes";
			}
			FillDBTypeDefinitionFromNormalData(toReturn, datatype, maxLength);
			return toReturn;
		}


		/// <summary>
		/// Determines the length in the string specified. Can either by (number) or (max), where 'number' is any positive integer.
		/// </summary>
		/// <param name="lengthWrapped"></param>
		/// <returns></returns>
		private int DetermineMaxLength(string lengthWrapped)
		{
			var lengthSpecification = lengthWrapped.Replace("(", "").Replace(")", "");
			if(lengthSpecification == "max")
			{
				return 0;
			}
			int.TryParse(lengthSpecification, out var length);
			return length;
		}


		/// <summary>
		/// Fills the DB type definition from normal data.
		/// </summary>
		/// <param name="toFill">To fill.</param>
		/// <param name="datatype">The datatype.</param>
		/// <param name="maxLength">Length of the max.</param>
		private void FillDBTypeDefinitionFromNormalData(DBTypeDefinition toFill, string datatype, int maxLength)
		{
			int length = maxLength;
			var dbType = this.DriverToUse.ConvertStringToDBType(datatype);
			toFill.SetDBType(dbType, this.DriverToUse, length, 0, 0);
		}


		#region Class Property Declaration
		/// <summary>
		/// Gets the size of the batch into which the work is divided. This number should be lower than the maximum number of parameters a driver could take.
		/// The default implementation returns 100
		/// </summary>
		public override int BatchSize
		{
			get
			{
				return 250;
			}
		}
		#endregion    
	}
}
