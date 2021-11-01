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
using System.Data;
using System.Text;
using SD.LLBLGen.Pro.DBDriverCore;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using SD.Tools.BCLExtensions.CollectionsRelated;
using SD.Tools.BCLExtensions.DataRelated;
using System.Text.RegularExpressions;
using Google.Cloud.Spanner.Data;
using SD.Tools.Algorithmia.GeneralDataStructures;

namespace SD.LLBLGen.Pro.DBDrivers.Spanner
{
	/// <summary>
	/// General implementation of the Google Cloud Spanner DBDriver.
	/// </summary>
	public class SpannerDBDriver : DBDriverBase
	{
		#region Constants
		private const string driverType = "Google Cloud Spanner DBDriver";
		private const string driverVendor = "Solutions Design bv";
		private const string driverCopyright = "(c)2002-2021 Solutions Design, all rights reserved.";
		private const string driverID = "00127E43-3911-4EA5-ABFF-7456C6349261";
		#endregion
		
		#region Members
		private MultiValueDictionary<string, Pair<string, string>> _parentChildTableRelationships;
		#endregion

		/// <summary>
		/// CTor
		/// </summary>
		public SpannerDBDriver() : base((int)SpannerDbTypes.NumberOfSqlDbTypes, driverType, driverVendor, GetDriverVersion(), driverCopyright, driverID, "initial catalog")
		{
			InitDataStructures();
			_parentChildTableRelationships = new MultiValueDictionary<string, Pair<string, string>>();
		}



		/// <summary>
		/// Fills the RDBMS functionality aspect list.
		/// </summary>
		protected override void FillRdbmsFunctionalityAspects()
		{
			this.RdbmsFunctionalityAspects.Add(RdbmsFunctionalityAspect.CentralUnitIsCatalog);
			this.RdbmsFunctionalityAspects.Add(RdbmsFunctionalityAspect.SupportsDeleteRules);
		}


		/// <summary>
		/// Fills the several DBType convertion arrays with the right content, so the conversion methods can work quickly.
		/// </summary>
		protected override void FillDbTypeConvertArrays()
		{
			this.DBTypesAsProviderType[(int)SpannerDbTypes.Bool] = "Bool";
			this.DBTypesAsProviderType[(int)SpannerDbTypes.Bytes] = "Bytes";
			this.DBTypesAsProviderType[(int)SpannerDbTypes.Date] = "Date";
			this.DBTypesAsProviderType[(int)SpannerDbTypes.Int64] = "Int64";
			this.DBTypesAsProviderType[(int)SpannerDbTypes.String] = "String";
			this.DBTypesAsProviderType[(int)SpannerDbTypes.Float64] = "Float64";
			this.DBTypesAsProviderType[(int)SpannerDbTypes.Timestamp] = "Timestamp";
			
			this.DBTypesAsNETType[(int)SpannerDbTypes.Bool] = typeof(bool);
			this.DBTypesAsNETType[(int)SpannerDbTypes.Date] = typeof(DateTime);
			this.DBTypesAsNETType[(int)SpannerDbTypes.Bytes] = typeof(Byte[]);
			this.DBTypesAsNETType[(int)SpannerDbTypes.Int64] = typeof(long);
			this.DBTypesAsNETType[(int)SpannerDbTypes.String] = typeof(string);
			this.DBTypesAsNETType[(int)SpannerDbTypes.Float64] = typeof(double);
			this.DBTypesAsNETType[(int)SpannerDbTypes.Timestamp] = typeof(DateTime);

			this.DBTypesAsString[(int)SpannerDbTypes.Bool] = "bool";
			this.DBTypesAsString[(int)SpannerDbTypes.Date] = "date";
			this.DBTypesAsString[(int)SpannerDbTypes.Bytes] = "bytes";
			this.DBTypesAsString[(int)SpannerDbTypes.Int64] = "int64";
			this.DBTypesAsString[(int)SpannerDbTypes.String] = "string";
			this.DBTypesAsString[(int)SpannerDbTypes.Float64] = "float64";
			this.DBTypesAsString[(int)SpannerDbTypes.Timestamp] = "timestamp";
		}


		/// <summary>
		/// Fills the NET to DB type conversions list.
		/// </summary>
		protected override void FillNETToDBTypeConversionsList()
		{
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(DateTime), (int)SpannerDbTypes.Timestamp, 0, 0, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(DateTime), (int)SpannerDbTypes.Date, 0, 0, 0));

			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(byte[]), (int)SpannerDbTypes.Bytes, -1, -1, -1));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(string), (int)SpannerDbTypes.String, -1, -1, -1));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(double), (int)SpannerDbTypes.Float64, 0, -1, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(long), (int)SpannerDbTypes.Int64, 0, -1, 0));
			this.NETToDBTypeConversions.Add(new NETToDBTypeConversion(typeof(bool), (int)SpannerDbTypes.Bool, 0, 0, 0));
		}


		/// <summary>
		/// Fills the DB type sort order list.
		/// </summary>
		protected override void FillDBTypeSortOrderList()
		{
			// datetime
			// the ADO.NET provider maps DbType.DateTime to Timestamp, so we prefer that too.
			this.SortOrderPerDBType.Add((int)SpannerDbTypes.Timestamp, 0);
			this.SortOrderPerDBType.Add((int)SpannerDbTypes.Date, 1);
		}


		/// <summary>
		/// Gets the decimal types for this driver. Used for optimizing the SortOrder per db type.
		/// </summary>
		/// <returns>
		/// List with the db type values requested or an empty list if not applicable
		/// </returns>
		protected override List<int> GetDecimalTypes()
		{
			return new List<int>();
		}

		/// <summary>
		/// Gets the currency types for this driver. Used for optimizing the SortOrder per db type.
		/// </summary>
		/// <returns>
		/// List with the db type values requested or an empty list if not applicable
		/// </returns>
		protected override List<int> GetCurrencyTypes()
		{
			return new List<int>();
		}

		/// <summary>
		/// Gets the fixed length types (with multiple bytes, like char, binary), not b/clobs, for this driver. Used for optimizing the SortOrder per db type.
		/// </summary>
		/// <returns>
		/// List with the db type values requested or an empty list if not applicable
		/// </returns>
		/// <remarks>It's essential that natural character types are stored at a lower index than normal character types.</remarks>
		protected override List<int> GetFixedLengthTypes()
		{
			return new List<int>();
		}

		/// <summary>
		/// Gets the variable length types (with multiple bytes, like varchar, varbinary), not b/clobs, for this driver. Used for optimizing the SortOrder per
		/// db type.
		/// </summary>
		/// <returns>
		/// List with the db type values requested or an empty list if not applicable
		/// </returns>
		/// <remarks>It's essential that natural character types are stored at a lower index than normal character types.</remarks>
		protected override List<int> GetVariableLengthTypes()
		{
			return new List<int> { (int)SpannerDbTypes.Bytes, (int)SpannerDbTypes.String};
		}


		/// <summary>
		/// Gets the string value of the db type passed in, from the Enum specification used in this driver for type specification
		/// </summary>
		/// <param name="dbType">The db type value.</param>
		/// <returns>string representation of the dbType specified when seen as a value in the type enum used by this driver to specify types.</returns>
		public override string GetDbTypeAsEnumStringValue(int dbType)
		{
			if(!Enum.IsDefined(typeof(SpannerDbTypes), dbType))
			{
				return "INVALID";
			}
			return ((SpannerDbTypes)dbType).ToString();
		}
		

		/// <summary>
		/// Converts the name specified to a DBType value.
		/// </summary>
		/// <param name="dbTypeName">Name of the db type.</param>
		/// <returns>
		/// the DBType value corresponding to the dbtype name specified
		/// </returns>
		public override int ConvertStringToDBType(string dbTypeName)
		{
			int toReturn;

			switch(dbTypeName.ToLowerInvariant())
			{
				case "bool":
					toReturn = (int)SpannerDbTypes.Bool;
					break;
				case "bytes":
					toReturn = (int)SpannerDbTypes.Bytes;
					break;
				case "date":
					toReturn = (int)SpannerDbTypes.Date;
					break;
				case "float64":
					toReturn = (int)SpannerDbTypes.Float64;
					break;
				case "int64":
					toReturn = (int)SpannerDbTypes.Int64;
					break;
				case "string":
					toReturn = (int)SpannerDbTypes.String;
					break;
				case "timestamp":
					toReturn = (int)SpannerDbTypes.Timestamp;
					break;
				default:
					toReturn = (int)SpannerDbTypes.String;
					break;
			}

			return toReturn;
		}


		/// <summary>
		/// Returns true if the passed in type is a numeric type. 
		/// </summary>
		/// <param name="dbType">type to check</param>
		/// <returns>true if the type is a numeric type, false otherwise</returns>
		public override bool DBTypeIsNumeric(int dbType)
		{
			bool toReturn = false;

			switch((SpannerDbTypes)dbType)
			{
				case SpannerDbTypes.Int64:
				case SpannerDbTypes.Float64:
					toReturn = true;
					break;
			}

			return toReturn;
		}


		/// <summary>
		/// Gets all catalog names from the database system connected through the specified connection elements set.
		/// </summary>
		/// <returns>
		/// List of all catalog names found in the connected system. By default it returns a list with 'Default' for systems which don't
		/// use catalogs.
		/// </returns>
		public override List<string> GetAllCatalogNames()
		{
			// return the database name specified in the connection args. 
			return new List<string> {this.ConnectionElements[ConnectionElement.CatalogName]};
		}


		/// <summary>
		/// Gets all table names in the schema in the catalog specified in the database system connected through the specified connection elements set.
		/// </summary>
		/// <param name="catalogName">Name of the catalog.</param>
		/// <param name="schemaName">Name of the schema.</param>
		/// <returns>
		/// List of all the table names (not synonyms) in the schema in the catalog specified. By default it returns an empty list.
		/// </returns>
		public override List<DBElementName> GetAllTableNames(string catalogName, string schemaName)
		{
			DbConnection connection = this.CreateConnection();
			return ExecuteWithActiveRecoveryStrategy(() =>
													 {
														 try
														 {
															 this.ParentChildTableRelationships.Clear();
															 connection.Open();
															 var cmd = connection.CreateCommand();
															 // empty schema as Spanner doesn't have catalogs nor schemas
															 cmd.CommandText = "SELECT TABLE_NAME, PARENT_TABLE_NAME, ON_DELETE_ACTION FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=''";
															 var adapter = this.CreateDataAdapter(cmd);
															 var results = new DataTable();
															 adapter.Fill(results);
															 var toReturn = new List<DBElementName>();
															 foreach(var row in results.AsEnumerable())
															 {
																 var tableName = row.Value<string>("TABLE_NAME");
																 toReturn.Add(new DBElementName(tableName));
																 if(!row.IsNull("PARENT_TABLE_NAME"))
																 {
																	 this.ParentChildTableRelationships.Add(row.Value<string>("PARENT_TABLE_NAME"), new Pair<string, string>(tableName, row.Value<string>("ON_DELETE_ACTION")));
																 }
															 }
															 return toReturn;
														 }
														 finally
														 {
															 connection.SafeClose(true);
														 }
													 });
		}


		/// <summary>
		/// Produces the DBCatalogRetriever instance to use for retrieving meta-data of a catalog.
		/// </summary>
		/// <returns>ready to use catalog retriever object</returns>
		public override DBCatalogRetriever CreateCatalogRetriever()
		{
			return new SpannerCatalogRetriever(this);
		}


		/// <summary>
		/// Creates the connectiondata object to be used to obtain the required information for connecting to the database.
		/// </summary>
		/// <returns></returns>
		public override ConnectionDataBase CreateConnectionDataCollector()
		{
			return new SpannerConnectionData(this);
		}


		/// <summary>
		/// Gets the target description of the target the driver is connected to, for display in a UI
		/// </summary>
		/// <returns>
		/// string usable to display in a UI which contains a description of the target the driver is connected to.
		/// </returns>
		public override string GetTargetDescription()
		{
			return string.Format("{0} (ProjectID: {1}. InstanceID: {2})", this.DBDriverType, this.ConnectionElements[ConnectionElement.ProjectID] ?? string.Empty,
								 this.ConnectionElements[ConnectionElement.InstanceID] ?? string.Empty);
		}


		/// <summary>
		/// Constructs a valid connection string from the ConnectionElements specified in this driver instance.
		/// </summary>
		/// <param name="connectionElementsToUse">The connection elements to use when producing the connection string</param>
		/// <returns>
		/// A valid connection string which is usable to connect to the database to work with.
		/// </returns>
		public override string ConstructConnectionString(Dictionary<ConnectionElement, string> connectionElementsToUse)
		{
			return string.Format("Data Source=projects/{0}/instances/{1}/databases/{2};EnableGetSchemaTable=true", connectionElementsToUse.GetValue(ConnectionElement.ProjectID) ?? string.Empty,
																						 connectionElementsToUse.GetValue(ConnectionElement.InstanceID) ?? string.Empty, 
																						 connectionElementsToUse.GetValue(ConnectionElement.CatalogName) ?? string.Empty);
		}


		/// <summary>
		/// Performs the post set connection elements work. By default that's nothing. Use this method to process settings set in the connection dialog.
		/// </summary>
		protected override void PerformPostSetConnectionElementsWork()
		{
			base.PerformPostSetConnectionElementsWork();
			if(this.ConnectionElements.ContainsKey(ConnectionElement.UseTransientErrorRecovery))
			{
				// use the defaults for now. 
				this.SetTransientErrorRecoveryStrategy(new SpannerRecoveryStrategy());
			}
		}

		
		/// <inheritdoc/>
		protected override DbProviderFactory ObtainDbProviderFactoryInstance()
		{
			return SpannerProviderFactory.Instance;
		}


		/// <inheritdoc />
		protected override string ConvertSchemaElementToFullNameForSqlQuery(IProjectElementMapTargetElement sourceElement)
		{
			return WrapIdentifier(sourceElement.Name);
		}


		/// <inheritdoc />
		protected override string WrapIdentifier(string toWrap)
		{
			if(toWrap.StartsWith("`") && toWrap.EndsWith("`")) 
			{
				return toWrap;
			}
			return "`" + toWrap + "`";
		}


		private static string GetDriverVersion()
		{
			return FileVersionInfo.GetVersionInfo(typeof(SpannerDBDriver).Assembly.Location).FileVersion;
		}
		

		#region Properties
		/// <summary>
		/// Gets the parent-child table relationships. Key is parent table name. Value is list of pairs, with value1 the child table and value2 the on delete rule: CASCADE or NO ACTION
		/// </summary>
		internal MultiValueDictionary<string, Pair<string, string>> ParentChildTableRelationships
		{
			get { return _parentChildTableRelationships; }
		}
		#endregion
	}
}