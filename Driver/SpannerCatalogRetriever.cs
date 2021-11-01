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
using System.Collections.Generic;
using SD.LLBLGen.Pro.DBDriverCore;
using System.Data.Common;
using System.Linq;
using SD.LLBLGen.Pro.Core;
using SD.Tools.Algorithmia.GeneralDataStructures;
using SD.Tools.BCLExtensions.CollectionsRelated;
using SD.Tools.BCLExtensions.DataRelated;

namespace SD.LLBLGen.Pro.DBDrivers.Spanner
{
    /// <summary>
    /// Google Cloud Spanner specific implementation of DBCatalogRetriever
    /// </summary>
	public class SpannerCatalogRetriever : DBCatalogRetriever
    {
		/// <summary>
		/// CTor
		/// </summary>
		/// <param name="driverToUse">The driver to use.</param>
        public SpannerCatalogRetriever(DBDriverBase driverToUse) : base(driverToUse)
        {
        }

		
		/// <summary>
		/// Produces the DBSchemaRetriever instance to use for retrieving meta-data of a schema.
		/// </summary>
		/// <returns>ready to use schema retriever</returns>
		protected override DBSchemaRetriever CreateSchemaRetriever()
		{
			return new SpannerSchemaRetriever(this);
		}


		/// <summary>
		/// Retrieves all Foreign keys.
		/// </summary>
		/// <param name="catalogMetaData">The catalog meta data.</param>
		private void RetrieveForeignKeys(DBCatalog catalogMetaData)
		{
			if(catalogMetaData.Schemas.Count <= 0)
			{
				return;
			}
			
			// Spanner doesn't have FKs, but does have interleaved tables. We're going to define an FK between Parent and Child table, on the PK fields in common of both.
			// The parent/child relationships are in the driver. 
			var parentChildRelationships = ((SpannerDBDriver)this.DriverToUse).ParentChildTableRelationships;
			// we've just 1 schema, so preselect it
			var schema = catalogMetaData.Schemas.First();
			foreach(var kvp in parentChildRelationships)
			{
				DBTable pkTable = schema.FindTableByName(kvp.Key, true);
				if(pkTable == null)
				{
					continue;
				}
				HashSet<Pair<string, string>> childTables = kvp.Value;
				foreach(var pair in childTables)
				{
					var fkTable = schema.FindTableByName(pair.Value1, true);
					if(fkTable == null)
					{
						continue;
					}

					var pkSidePkFields = pkTable.PrimaryKeyFields.ToList();
					var fkSidePkFields = fkTable.PrimaryKeyFields.ToList();
					if(pkSidePkFields.Count > fkSidePkFields.Count)
					{
						// invalid
						continue;
					}
					var fkConstraint = new DBForeignKeyConstraint(string.Format("FK_{0}_{1}", pkTable.Name, fkTable.Name));
					fkConstraint.AppliesToTable = fkTable;
					fkTable.ForeignKeyConstraints.Add(fkConstraint);
					switch(pair.Value2.ToLowerInvariant())
					{
						case "cascade":
							fkConstraint.DeleteRuleAction = ForeignKeyRuleAction.Cascade;
							break;
						case "no action":
							fkConstraint.DeleteRuleAction = ForeignKeyRuleAction.NoAction;
							break;
					}
					for(int i = 0; i < pkSidePkFields.Count; i++)
					{
						fkConstraint.PrimaryKeyFields.Add(pkSidePkFields[i]);
						fkConstraint.ForeignKeyFields.Add(fkSidePkFields[i]);
					}
				}
			}
		}


		/// <summary>
		/// Produces the additional actions to perform by this catalog retriever
		/// </summary>
		/// <returns>list of additional actions to perform per schema</returns>
		private List<CatalogMetaDataRetrievalActionDescription> ProduceAdditionalActionsToPerform()
		{
			List<CatalogMetaDataRetrievalActionDescription> toReturn = new List<CatalogMetaDataRetrievalActionDescription>();
			toReturn.Add(new CatalogMetaDataRetrievalActionDescription("Retrieving all Foreign Key Constraints", (catalog) => RetrieveForeignKeys(catalog), false));
			return toReturn;
		}


		#region Class Property Declarations
		/// <summary>
		/// Gets the additional actions to perform per schema.
		/// </summary>
		protected override List<CatalogMetaDataRetrievalActionDescription> AdditionalActionsPerSchema
		{
			get { return ProduceAdditionalActionsToPerform(); }
		}
		#endregion
	}
}
