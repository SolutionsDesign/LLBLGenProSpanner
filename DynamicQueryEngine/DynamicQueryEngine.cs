//////////////////////////////////////////////////////////////////////
// Part of the Dynamic Query Engine (DQE) for Google Cloud Spanner, used in the generated code. 
// LLBLGen Pro is (c) 2002-2019 Solutions Design. All rights reserved.
// https://www.llblgen.com
//////////////////////////////////////////////////////////////////////
// The sourcecode for this DQE has been made available to LLBLGen Pro licensees
// so they can modify, update and/or extend it. Distribution of this sourcecode in textual, non-compiled, 
// non-binary form to non-licensees is prohibited. Distribution of binary compiled versions of this 
// sourcecode to non-licensees has been granted under the following license.
//////////////////////////////////////////////////////////////////////
// COPYRIGHTS:
// Copyright (c)2002-2019 Solutions Design. All rights reserved.
// https://www.llblgen.com
// 
// This DQE's sourcecode is released to LLBLGen Pro licensees under the following license:
// --------------------------------------------------------------------------------------------
// 
// Redistribution and use of the sourcecode in compiled, binary forms, with or without modification, 
// are permitted provided that the following conditions are met: 
//
// 1) Redistributions must reproduce the above copyright notice, this list of 
//    conditions and the following disclaimer in the documentation and/or other materials 
//    provided with the distribution. 
// 2) Redistribution of the sourcecode in textual, non-binary, non-compiled form is prohibited.
// 
// THIS SOFTWARE IS PROVIDED BY SOLUTIONS DESIGN ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, 
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A 
// PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL SOLUTIONS DESIGN OR CONTRIBUTORS BE LIABLE FOR 
// ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT 
// NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR 
// BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, 
// STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE 
// USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE. 
//
// The views and conclusions contained in the software and documentation are those of the authors 
// and should not be interpreted as representing official policies, either expressed or implied, 
// of Solutions Design.
//////////////////////////////////////////////////////////////////////
// Contributers to the code:
//		- Frans Bouma [FB]
//////////////////////////////////////////////////////////////////////
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Text.RegularExpressions;
using System.Collections.Specialized;
using System.Configuration;
using System.Data.Common;
using SD.LLBLGen.Pro.ORMSupportClasses;
using System.Linq;
using System.Reflection;
using SD.LLBLGen.Pro.QuerySpec;

namespace SD.LLBLGen.Pro.DQE.Spanner
{
	/// <summary>
	/// DynamicQueryEngine for Google Cloud Spanner.
	/// </summary>
	public class DynamicQueryEngine : DynamicQueryEngineBase
	{
		#region Static members
		private static readonly Dictionary<string, string> _schemaOverwrites = new Dictionary<string, string>();
		private static readonly FunctionMappingStore _functionMappings = new FunctionMappingStore();
		#endregion

        /// <summary>
        /// Static CTor for initializing TraceSwitch
        /// </summary>
        static DynamicQueryEngine()
        {
	        Switch = new TraceSwitch("SpannerDQE", "Tracer for Google Cloud Spanner Dynamic Query Engine");
			CreateFunctionMappingStore();
        }


		/// <summary>
		/// Configures the static members with the configuration data specified
		/// </summary>
		/// <param name="configuration"></param>
		internal static void Configure(SpannerDQEConfiguration configuration)
		{
			if(configuration == null)
			{
				return;
			}
			if(configuration.TraceLevel.HasValue)
			{
				Switch.Level = configuration.TraceLevel.Value;
			}
			// first merge the factories
			DbProviderFactoryInfo.MergeDbProviderFactories(configuration.DbProviderFactories);
			// then set the provider factory parameter data as it relies on the factories being there. 
			SpannerSpecificCreator.SetDbProviderFactoryParameterData("Google.Cloud.Spanner.Data.SpannerDbType", "SpannerDbType");
		}


		/// <summary>
		/// Creates a new Insert Query object which is ready to use.
		/// </summary>
		/// <param name="fields">Array of EntityFieldCore objects to use to build the insert query</param>
		/// <param name="fieldsPersistenceInfo">Array of IFieldPersistenceInfo objects to use to build the insert query</param>
		/// <param name="query">The query object to fill.</param>
		/// <param name="fieldToParameter">Hashtable which will contain after the call for each field the parameter which contains or will contain the field's value.</param>
		/// <remarks>Generic version.</remarks>
		/// <exception cref="System.ArgumentNullException">When fields is null or fieldsPersistenceInfo is null</exception>
		/// <exception cref="System.ArgumentException">When fields contains no EntityFieldCore instances or fieldsPersistenceInfo is empty.</exception>
		/// <exception cref="ORMQueryConstructionException">When there are no fields to insert in the fields list. This exception is to prevent
		/// INSERT INTO table () VALUES () style queries.</exception>
		protected override void CreateSingleTargetInsertDQ(IEntityFieldCore[] fields, IFieldPersistenceInfo[] fieldsPersistenceInfo,
															IActionQuery query, Dictionary<IEntityFieldCore, DbParameter> fieldToParameter )
		{
			TraceHelper.WriteLineIf(Switch.TraceInfo, "CreateSingleTargetInsertDQ", "Method Enter");
			QueryFragments fragments = new QueryFragments();
			fragments.AddFragments("INSERT INTO", this.Creator.CreateObjectName(fieldsPersistenceInfo[0]));
			var fieldNames = fragments.AddCommaDelimitedQueryFragments(true, fields?.Length ?? 0);
			fragments.AddFragment("VALUES");
			var valueFragments = fragments.AddCommaDelimitedQueryFragments(true, fields?.Length ?? 0);

			for (int i = 0; i < fields.Length; i++)
			{
				IEntityFieldCore field = fields[i];
				IFieldPersistenceInfo persistenceInfo = fieldsPersistenceInfo[i];
				bool isAutoTimestamp = false;
				if(!CheckIfFieldNeedsInsertAction(field))
				{
					// if the type is timestamp, we'll accept the field, but have to call a function instead of passing a value through a parameter. 
					if(persistenceInfo.SourceColumnDbType == "Timestamp" && field.IsReadOnly)
					{
						isAutoTimestamp = true;
					}
					else
					{
						// no insert value needed
						continue;
					}
				}
				var fieldName = this.Creator.CreateFieldNameSimple(persistenceInfo, field.Name);
				fieldNames.AddFragment(fieldName);
				if(isAutoTimestamp)
				{
					valueFragments.AddFragment("PENDING_COMMIT_TIMESTAMP()");
				}
				else
				{
					AppendFieldToValueFragmentsForInsert(query, fieldToParameter, valueFragments, field, persistenceInfo);
				}
			}
			if(fieldNames.Count <= 0)
			{
				throw new ORMQueryConstructionException("The insert query doesn't contain any fields.");
			}
			query.SetCommandText(fragments.ToString());

			TraceHelper.WriteIf(Switch.TraceVerbose, query, "Generated Sql query");
			TraceHelper.WriteLineIf(Switch.TraceInfo, "CreateSingleTargetInsertDQ", "Method Exit");
		}


		/// <summary>
		/// Creates a new Update Query object which is ready to use. Only 'changed' EntityFieldCore are included in the update query.
		/// Primary Key fields are never updated.
		/// </summary>
		/// <param name="fields">Array of EntityFieldCore objects to use to build the insert query</param>
		/// <param name="fieldsPersistenceInfo">Array of IFieldPersistenceInfo objects to use to build the update query</param>
		/// <param name="query">The query object to fill.</param>
		/// <param name="updateFilter">A complete IPredicate implementing object which contains the filter for the rows to update</param>
		/// <param name="relationsToWalk">list of EntityRelation objects, which will be used to formulate a FROM clause with INNER JOINs.</param>
		/// <exception cref="System.ArgumentNullException">When fields is null or when updateFilter is null or
		/// when relationsToWalk is null or when fieldsPersistence is null</exception>
		/// <exception cref="System.ArgumentException">When fields contains no EntityFieldCore instances or fieldsPersistenceInfo is empty.</exception>
		protected override void CreateSingleTargetUpdateDQ(IEntityFieldCore[] fields, IFieldPersistenceInfo[] fieldsPersistenceInfo, 
														   IActionQuery query, IPredicate updateFilter, IRelationCollection relationsToWalk)
		{
			this.CreateSingleTargetUpdateDQUsingCorrelatedSubQuery(fields, fieldsPersistenceInfo, query, updateFilter, relationsToWalk, false);
		}


		/// <inheritdoc />
		protected override IRetrievalQuery CreatePagingSelectDQ(QueryParameters parameters, DbConnection connectionToUse)
		{
			TraceHelper.WriteLineIf(Switch.TraceInfo, "CreatePagingSelectDQ", "Method Enter");

			int max = 0;
			bool pagingRequired = true;
			if(parameters.RowsToSkip <= 0)
			{
				// no paging.
				max = parameters.RowsToTake;
				pagingRequired = false;
			}
			
			var rowsToTakeSave = parameters.RowsToTake;
			parameters.RowsToTake = max;
			IRetrievalQuery normalQuery = this.CreateSelectDQImpl(parameters, connectionToUse);
			parameters.RowsToTake = rowsToTakeSave;
			
			if(!pagingRequired)
			{
				TraceHelper.WriteLineIf(Switch.TraceInfo, "CreatePagingSelectDQ: no paging.", "Method Exit");
				return normalQuery;
			}
			bool emitQueryToTrace = false;
			if (normalQuery.RequiresClientSideDistinctFiltering)
			{
				// manual paging required
				normalQuery.RequiresClientSidePaging = pagingRequired;
				normalQuery.ManualRowsToSkip = parameters.RowsToSkip;
				normalQuery.ManualRowsToTake = parameters.RowsToTake;
			}
			else
            {
                // normal paging. Embed paging logic. 
                // There is no LIMIT statement in the query as we've passed '0' for maxAmountOfItemsToReturn
	            WrapCommandInPagingQuery(normalQuery, parameters.RowsToSkip, parameters.RowsToTake, parameters.WithTies && parameters.SortClauseSpecified);
				emitQueryToTrace = true;
			}

			TraceHelper.WriteIf(Switch.TraceVerbose && emitQueryToTrace, Switch.TraceVerbose?normalQuery.ToString():string.Empty, "Generated Sql query");
			TraceHelper.WriteLineIf(Switch.TraceInfo, "CreatePagingSelectDQ", "Method Exit");

			return normalQuery;
		}


		/// <inheritdoc />
		protected override void WrapCommandInPagingQuery(IRetrievalQuery query, int offset, int limit, bool withTiesRequired)
		{
			if(offset <= 0 && limit <= 0)
			{
				return;
			}
			string limitSnippet = (limit > 0) ? " LIMIT " + limit : string.Empty;
			query.Command.CommandText = query.Command.CommandText + limitSnippet + " OFFSET " + offset;
		}


		/// <inheritdoc />
		protected override List<DbParameter> AddSingleTargetElementToFromClause(IEntityFieldCore[] selectList, IRelationCollection relationsToWalk, IFieldPersistenceInfo persistenceInfoToUse, QueryFragments fromClause)
		{
			string objectName = this.Creator.CreateObjectName(persistenceInfoToUse);
			string tableHintFragment = string.Empty;
			string joinHintFragment = string.Empty;
			List<DbParameter> joinHintParameters = null;
			if(relationsToWalk != null)
			{
				// use temporal elements as join hint carrier in this case. 
				((IDbSpecificHintCreator)this.Creator).ProduceFromClauseDirectiveFragments(relationsToWalk.FromClauseDirectives, out tableHintFragment, out joinHintFragment,
																						   out joinHintParameters);
			}
			// emit join hints before the table name, and table hints after the table name.
			fromClause.AddFragment(joinHintFragment);
			fromClause.AddFragment(objectName);
			fromClause.AddFragment(tableHintFragment);
			string targetAlias = this.DetermineTargetAlias(selectList[0], relationsToWalk);
			if(targetAlias.Length > 0)
			{
				fromClause.AddFragment(this.Creator.CreateValidAlias(targetAlias));
			}
			return joinHintParameters;
		}


		/// <summary>
		/// Creates a new IDbSpecificCreator and initializes it
		/// </summary>
		/// <returns></returns>
		protected override IDbSpecificCreator CreateDbSpecificCreator()
		{
			return new SpannerSpecificCreator();
		}


		/// <summary>
		/// Creates the function mappings for this DQE.
		/// </summary>
		private static void CreateFunctionMappingStore()
		{
			// Parameter 0 is the element the method is called on. parameter 1 is the first argument etc. 
			// All indexes are converted to 0-based.
			////////////////////////////////////////////////
			// VB.NET compiler services specific methods
			////////////////////////////////////////////////
			// CompareString(3), which is emitted by the VB.NET compiler when a '=' operator is used between two string-typed operands.
			_functionMappings.Add(new FunctionMapping("Microsoft.VisualBasic.CompilerServices.Operators", "CompareString", 3, "CASE WHEN {0} < {1} THEN -1 WHEN {0} = {1} THEN 0 ELSE 1 END"));
			_functionMappings.Add(new FunctionMapping("Microsoft.VisualBasic.CompilerServices.EmbeddedOperators", "CompareString", 3, "CASE WHEN {0} < {1} THEN -1 WHEN {0} = {1} THEN 0 ELSE 1 END"));

			////////////////////////////////////////////////
			// Array related properties
			////////////////////////////////////////////////
			// Length
			_functionMappings.Add(new FunctionMapping(typeof(Array), "get_Length", 0, "ARRAY_LENGTH({0})"));

			////////////////////////////////////////////////
			// Boolean related functions
			////////////////////////////////////////////////
			// Negate(1) (!operand)
			_functionMappings.Add(new FunctionMapping(typeof(bool), "Negate", 1, "NOT ({0}=1)"));
			// ToString()
			_functionMappings.Add(new FunctionMapping(typeof(bool), "ToString", 0, "CASE WHEN ({0})=1 THEN 'True' ELSE 'False' END"));


			////////////////////////////////////////////////
			// Convert related functions
			////////////////////////////////////////////////
			// ToBoolean(1)
			_functionMappings.Add(new FunctionMapping(typeof(Convert), "ToBoolean", 1, "(CAST({0} AS Boolean)=1)"));
			// ToDateTime(1)
			_functionMappings.Add(new FunctionMapping(typeof(Convert), "ToDateTime", 1, "CAST({0} AS Timestamp)"));
			// ToDouble(1)
			_functionMappings.Add(new FunctionMapping(typeof(Convert), "ToDouble", 1, "CAST({0} AS Float64)"));
			// ToInt64(1)
			_functionMappings.Add(new FunctionMapping(typeof(Convert), "ToInt64", 1, "CAST({0} AS Int64)"));
			// ToString(1)
			_functionMappings.Add(new FunctionMapping(typeof(Convert), "ToString", 1, "CAST({0} AS String)"));

			////////////////////////////////////////////////
			// Int64 related functions
			////////////////////////////////////////////////
			_functionMappings.Add(new FunctionMapping(typeof(Int64), "ToString", 0, "CAST({0} AS String)"));
			_functionMappings.Add(new FunctionMapping(typeof(UInt64), "ToString", 0, "CAST({0} AS String)"));

			////////////////////////////////////////////////
			// DateTime related functions
			////////////////////////////////////////////////
			// make_interval is faster, but supported on 9.4+, and we have to support 9.3 (which is still in support) as well, so we can't use that.
			// AddDays(1)
			_functionMappings.Add(new FunctionMapping(typeof(DateTime), "AddDays", 1, "DATE_ADD({0}, INTERVAL {1} DAY)"));
			// AddHours(1)
			_functionMappings.Add(new FunctionMapping(typeof(DateTime), "AddHours", 1, "TIMESTAMP_ADD({0}, INTERVAL {1} HOUR)"));
			// AddMinutes(1)
			_functionMappings.Add(new FunctionMapping(typeof(DateTime), "AddMinutes", 1, "TIMESTAMP_ADD({0}, INTERVAL {1} MINUTE)"));
			// AddMonths(1)
			_functionMappings.Add(new FunctionMapping(typeof(DateTime), "AddMonths", 1, "DATE_ADD({0}, INTERVAL {1} MONTH)"));
			// AddSeconds(1)
			_functionMappings.Add(new FunctionMapping(typeof(DateTime), "AddSeconds", 1, "TIMESTAMP_ADD({0}, INTERVAL {1} SECOND)"));
			// AddMilliseconds(1)
			_functionMappings.Add(new FunctionMapping(typeof(DateTime), "AddMilliseconds", 1, "TIMESTAMP_ADD({0}, INTERVAL {1} MILLISECOND)"));
			// AddYears(1)
			_functionMappings.Add(new FunctionMapping(typeof(DateTime), "AddYears", 1, "DATE_ADD({0}, INTERVAL {1} YEAR)"));
			// Compare(2)
			_functionMappings.Add(new FunctionMapping(typeof(DateTime), "Compare", 2, "CASE WHEN {0} < {1} THEN -1 WHEN {0} = {1} THEN 0 ELSE 1 END"));

			////////////////////////////////////////////////
			// DateTime related properties
			////////////////////////////////////////////////
			// Date
			_functionMappings.Add(new FunctionMapping(typeof(DateTime), "get_Date", 0, "{0}"));
			// Day
			_functionMappings.Add(new FunctionMapping(typeof(DateTime), "get_Day", 0, "EXTRACT(DAY FROM {0})"));
			// DayOfWeek
			_functionMappings.Add(new FunctionMapping(typeof(DateTime), "get_DayOfWeek", 0, "EXTRACT(DAYOFWEEK FROM {0})"));
			// DayOfYear
			_functionMappings.Add(new FunctionMapping(typeof(DateTime), "get_DayOfYear", 0, "EXTRACT(DAYOFYEAR FROM {0})"));
			// Hour
			_functionMappings.Add(new FunctionMapping(typeof(DateTime), "get_Hour", 0, "EXTRACT(HOUR FROM {0})"));
			// Millisecond
			_functionMappings.Add(new FunctionMapping(typeof(DateTime), "get_Millisecond", 0, "EXTRACT(MILLISECONDS FROM {0})"));
			// Minute
			_functionMappings.Add(new FunctionMapping(typeof(DateTime), "get_Minute", 0, "EXTRACT(MINUTE FROM {0})"));
			// Month
			_functionMappings.Add(new FunctionMapping(typeof(DateTime), "get_Month", 0, "EXTRACT(MONTH FROM {0})"));
			// Second
			_functionMappings.Add(new FunctionMapping(typeof(DateTime), "get_Second", 0, "EXTRACT(SECOND FROM {0})"));
			// Year
			_functionMappings.Add(new FunctionMapping(typeof(DateTime), "get_Year", 0, "EXTRACT(YEAR FROM {0})"));
			
			////////////////////////////////////////////////
			// Math related functions
			////////////////////////////////////////////////
			// Abs
			_functionMappings.Add(new FunctionMapping(typeof(Math), "Abs", 1, "ABS({0})"));
			// Sign 
			_functionMappings.Add(new FunctionMapping(typeof(Math), "Sign", 1, "SIGN({0})"));
			// IEEERemainder
			_functionMappings.Add(new FunctionMapping(typeof(Math), "IEEERemainder", 2, "IEE_DIVIDE({0}, {1})"));
			// Sqrt
			_functionMappings.Add(new FunctionMapping(typeof(Math), "Sqrt", 1, "SQRT({0})"));
			// Exp
			_functionMappings.Add(new FunctionMapping(typeof(Math), "Exp", 1, "EXP({0})"));
			// Log
			_functionMappings.Add(new FunctionMapping(typeof(Math), "Log", 1, "LN({0})"));
			// Log
			_functionMappings.Add(new FunctionMapping(typeof(Math), "Log", 2, "LOG({0}, {1})"));
			// Log10
			_functionMappings.Add(new FunctionMapping(typeof(Math), "Log10", 1, "LOG10({0})"));
			// Pow
			_functionMappings.Add(new FunctionMapping(typeof(Math), "Pow", 2, "POWER({0}, {1})"));
			// Round
			_functionMappings.Add(new FunctionMapping(typeof(Math), "Round", 1, "ROUND({0})"));
			// Round
			_functionMappings.Add(new FunctionMapping(typeof(Math), "Round", 2, "ROUND({0}, {1})"));
			// Trunc
			_functionMappings.Add(new FunctionMapping(typeof(Math), "Trunc", 1, "TRUNC({0})"));
			// Ceiling
			_functionMappings.Add(new FunctionMapping(typeof(Math), "Ceiling", 1, "CEIL({0})"));
			// Floor 
			_functionMappings.Add(new FunctionMapping(typeof(Math), "Floor", 1, "FLOOR({0})"));

			////////////////////////////////////////////////
			// String related functions
			////////////////////////////////////////////////
			// Compare(2)
			_functionMappings.Add(new FunctionMapping(typeof(string), "Compare", 2, "CASE WHEN {0} < {1} THEN -1 WHEN {0} = {1} THEN 0 ELSE 1 END"));
			// Concat(2).
			_functionMappings.Add(new FunctionMapping(typeof(string), "Concat", 2, "CONCAT({0},{1})"));
			// IndexOf(1).
			_functionMappings.Add(new FunctionMapping(typeof(string), "IndexOf", 1, "STRPOS({0}, {1}) - 1"));
			// PadLeft(1)
			_functionMappings.Add(new FunctionMapping(typeof(string), "PadLeft", 1, "CASE WHEN CHAR_LENGTH({0})>={1} THEN {0} ELSE LPAD({0}, {1}) END"));
			// PadLeft(2)
			_functionMappings.Add(new FunctionMapping(typeof(string), "PadLeft", 2, "CASE WHEN CHAR_LENGTH({0})>={1} THEN {0} ELSE LPAD({0}, {1}, {2}) END"));
			// PadRight(1)
			_functionMappings.Add(new FunctionMapping(typeof(string), "PadRight", 1, "CASE WHEN CHAR_LENGTH({0})>={1} THEN {0} ELSE RPAD({0}, {1}) END"));
			// PadRight(2)
			_functionMappings.Add(new FunctionMapping(typeof(string), "PadRight", 2, "CASE WHEN CHAR_LENGTH({0})>={1} THEN {0} ELSE RPAD({0}, {1}, {2}) END"));
			// Remove(1)
			_functionMappings.Add(new FunctionMapping(typeof(string), "Remove", 1, "SUBSTR({0}, 1, {1})"));
			// Remove(2)
			_functionMappings.Add(new FunctionMapping(typeof(string), "Remove", 2, "CONCAT(SUBSTR({0}, 1, {1}), SUBSTR({0}, {1}+1+{2}))"));
			// Replace(2)
			_functionMappings.Add(new FunctionMapping(typeof(string), "Replace", 2, "REPLACE({0}, {1}, {2})"));
			// Substring(1)
			_functionMappings.Add(new FunctionMapping(typeof(string), "Substring", 1, "SUBSTR({0}, {1}+1)"));
			// Substring(2)
			_functionMappings.Add(new FunctionMapping(typeof(string), "Substring", 2, "SUBSTR({0}, {1}+1, {2})"));
			// ToLower(0)
			_functionMappings.Add(new FunctionMapping(typeof(string), "ToLower", 0, "LOWER({0})"));
			// ToUpper(0)
			_functionMappings.Add(new FunctionMapping(typeof(string), "ToUpper", 0, "UPPER({0})"));
			// Trim(0)
			_functionMappings.Add(new FunctionMapping(typeof(string), "Trim", 0, "TRIM({0})"));
			////////////////////////////////////////////////
			// String related properties
			////////////////////////////////////////////////
			// Length
			_functionMappings.Add(new FunctionMapping(typeof(string), "get_Length", 0, "CHAR_LENGTH({0})"));
			// Chars(1) / indexer
			_functionMappings.Add(new FunctionMapping(typeof(string), "get_Chars", 1, "SUBSTR({0}, {1}+1, 1) "));

			////////////////////////////////////////////////
			// Object related functions
			////////////////////////////////////////////////
			// IIF(3). (IIF(op1, op2, op3) / op1 ? op2 : op3 statement)
			_functionMappings.Add(new FunctionMapping(typeof(object), "IIF", 3, "IF({0}, {1}, {2})"));
			// IIF(3). (IIF(op1, op2, op3) / op1 ? op2 : op3 statement). Used for boolean operands. The IIF will end up being wrapped with a boolean wrapper anyway, so it has to produce a boolean 
			_functionMappings.Add(new FunctionMapping(typeof(object), "IIF_Bool", 3, "(CASE WHEN {0}=1 THEN {1} ELSE {2} END)=1"));
			// LeftShift(2). (op1 << op2)
			_functionMappings.Add(new FunctionMapping(typeof(object), "LeftShift", 2, "({0} << {1})"));
			// RightShift(2). (op1 >> op2)
			_functionMappings.Add(new FunctionMapping(typeof(object), "RightShift", 2, "({0} >> {1})"));
			// BooleanInProjectionWrapper(1)
			_functionMappings.Add(new FunctionMapping(typeof(object), "BooleanInProjectionWrapper", 1, "CASE WHEN {0} THEN 1 ELSE 0 END"));
		}


		#region Class Property Declarations
		
		/// <inheritdoc />
		public override bool SupportsPackedQueries 
		{
			get { return true; }
		}

		/// <summary>
		/// Gets the function mappings for the particular DQE. These function mappings are static and therefore not changeable.
		/// </summary>
		public override FunctionMappingStore FunctionMappings
		{
			get
			{
				return _functionMappings;
			}
		}
		#endregion
    }
}
