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
using System.Data;

using SD.LLBLGen.Pro.ORMSupportClasses;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace SD.LLBLGen.Pro.DQE.Spanner
{
    /// <summary>
    /// Implements IDbSpecificCreator for Google Cloud Spanner. 
    /// </summary>
	[Serializable]
    public class SpannerSpecificCreator : DbSpecificCreatorBase
    {
        #region Statics
        // this info is defined here and not in the base class because now a user can use more than one DQE at the same time with different providers.
        private static readonly DbProviderFactoryInfo _dbProviderFactoryInfo = new DbProviderFactoryInfo();
		private static StringFragmentsCache _stringFragmentsCache = new StringFragmentsCache();
		private static DynamicQueryEngineBase _cachedDQE = new DynamicQueryEngine();
        #endregion

		/// <summary>
        /// Sets the db provider factory parameter data. This will influence which DbProviderFactory is used and which enum types the field persistence info
        /// field type names are resolved to.
        /// </summary>
        /// <param name="dbProviderSpecificEnumTypeName">Name of the db provider specific enum type.</param>
        /// <param name="dbProviderSpecificEnumTypePropertyName">Name of the db provider specific enum type property.</param>
        public static void SetDbProviderFactoryParameterData(string dbProviderSpecificEnumTypeName, string dbProviderSpecificEnumTypePropertyName)
        {
            _dbProviderFactoryInfo.SetDbProviderFactoryParameterData("Google.Cloud.Spanner.Data", dbProviderSpecificEnumTypeName, dbProviderSpecificEnumTypePropertyName);
        }


        /// <summary>
		/// Routine which creates a valid alias string for the raw alias passed in. For example, the alias will be surrounded by "[]" on sqlserver. 
		/// Used by the RelationCollection to produce a valid alias for joins.
		/// </summary>
		/// <param name="rawAlias">the raw alias to make valid</param>
		/// <returns>valid alias string to use.</returns>
		public override string CreateValidAlias(string rawAlias)
        {
			if(string.IsNullOrEmpty(rawAlias))
			{
				return rawAlias;
			}
			if(rawAlias[0] == '`')
			{
				return rawAlias;
			}
			return string.Concat("`", rawAlias, "`");
		}


		/// <summary>
		/// Routine which creates a valid identifier string for the plain identifier string passed in and appends the fragments to the queryfragments specified. 
		/// For example, the identifier will be surrounded by "[]" on sqlserver. If the specified rawIdentifier needs wrapping with e.g. [], the [ and ] characters are
		/// added as separate fragments to toAppendTo so no string concatenation has to take place. Use this method over CreateValidAlias if possible.
		/// </summary>
		/// <param name="toAppendTo">the fragments container to append the fragments to.</param>
		/// <param name="rawIdentifier">the plain identifier string to make valid</param>
		public override void AppendValidIdentifier(QueryFragments toAppendTo, string rawIdentifier)
		{
			if(string.IsNullOrEmpty(rawIdentifier))
			{
				return;
			}
			if(rawIdentifier[0] == '`')
			{
				toAppendTo.AddFragment(rawIdentifier);
			}
			else
			{
				toAppendTo.AddStringFragmentsAsSingleUnit("`", rawIdentifier, "`");
			}
		}


		/// <summary>
		/// Determines the db type name for value.
		/// </summary>
		/// <param name="value">The value.</param>
		/// <param name="realValueToUse">The real value to use. Normally it's the same as value, but in cases where value as a type isn't supported, the 
		/// value is converted to a value which is supported.</param>
		/// <returns>The name of the provider specific DbType enum name for the value specified</returns>
		public override string DetermineDbTypeNameForValue(object value, out object realValueToUse)
        {
            realValueToUse = value;
            string dbTypeToUse;
            if((value == null) || (value == DBNull.Value))
            {
                // don't set a type
                dbTypeToUse = string.Empty;
            }
            else
            {
                switch(value.GetType().UnderlyingSystemType.FullName)
                {
                    case "System.String":
                        dbTypeToUse = "String";
                        break;
                    case "System.Int64":
					case "System.Int32":
					case "System.Int16":
					case "System.Byte":
					case "System.UInt64":
					case "System.UInt32":
					case "System.UInt16":
					case "System.SByte":
                        dbTypeToUse = "Int64";
                        break;
                    case "System.DateTime":
                        dbTypeToUse = "Timestamp";
                        break;
                    case "System.Boolean":
                        dbTypeToUse = "Bool";
                        break;
                    case "System.Byte[]":
                        dbTypeToUse = "Bytes";
                        break;
					case "System.Single":
					case "System.Double":
						dbTypeToUse = "Float64";
						break;
                    default:
                        dbTypeToUse = "String";
                        break;
                }
            }
            return dbTypeToUse;
        }


        /// <summary>
        /// Creates a valid Parameter for the pattern in a LIKE statement. This is a special case, because it shouldn't rely on the type of the
        /// field the LIKE statement is used with but should be the unicode varchar type.
        /// </summary>
        /// <param name="pattern">The pattern to be passed as the value for the parameter. Is used to determine length of the parameter.</param>
        /// <param name="targetFieldDbType">Type of the target field db, in provider specific enum string format (e.g. "Int" for SqlDbType.Int)</param>
        /// <returns>
        /// Valid parameter for usage with the target database.
        /// </returns>
        public override DbParameter CreateLikeParameter(string pattern, string targetFieldDbType)
        {
            return this.CreateParameter("String", pattern.Length, ParameterDirection.Input, false, 0, 0, pattern);
        }


        /// <summary>
        /// Creates a valid object name (e.g. a name for a table or view) based on the fragments specified. The name is ready to  use and contains
        /// all alias wrappings required. 
        /// </summary>
        /// <param name="catalogName">Name of the catalog.</param>
        /// <param name="schemaName">Name of the schema.</param>
        /// <param name="elementName">Name of the element.</param>
        /// <returns>valid object name</returns>
        public override string CreateObjectName(string catalogName, string schemaName, string elementName)
        {
			var key = new LightweightStringFragmentCacheKey(string.Empty, elementName);
			var toReturn = _stringFragmentsCache.GetConcatenatedResult(key);
			if(toReturn == null)
			{
				var fragments = new QueryFragments(String.Empty, 1);
				AppendValidIdentifier(fragments, elementName);
				toReturn = fragments.ToString();
				_stringFragmentsCache.Add(key, toReturn);
			}
			return toReturn;
		}
        

        /// <summary>
        /// Produces the from clause directive fragments from the specified fromClauseElementDirectives.
        /// </summary>
        /// <param name="fromClauseElementDirectives">From clause element directives to use to produce the fragments requested. If null, only global setting driven hints are produced.</param>
        /// <param name="tableHintFragment">The table hint fragment produced. Can be empty string.</param>
        /// <param name="joinHintFragment">The join hint fragment. Can be empty string.</param>
        /// <param name="joinHintParameters">The join hint parameters (if any) created for the join hint Fragment. If no parameters are created, this
        /// parameter is null</param>
        public override void ProduceFromClauseDirectiveFragments(IEnumerable<FromClauseElementDirective> fromClauseElementDirectives, out string tableHintFragment,
                                                                 out string joinHintFragment, out List<DbParameter> joinHintParameters)
        {
			joinHintParameters = null;
            joinHintFragment = string.Empty;
            tableHintFragment = string.Empty;
			if(fromClauseElementDirectives != null && fromClauseElementDirectives.Any())
			{
				var tableHintFragments = new QueryFragments(", ") { Prefix = "@{", Suffix = "}" };
				var joinHintFragments = new QueryFragments(", ") { Prefix = "@{", Suffix = "}" };
				// as there's just 1 temporal predicate possible, last one wins. 
				foreach(var fcd in fromClauseElementDirectives)
				{
					var fragment = fcd.ToQueryText(this, out var _);
					switch(fcd.DirectiveType)
					{
						case FromClauseElementDirectiveType.TableViewHint:
							// add the force_index hint to the table hints, the rest to the join hints. 
							if(fragment.StartsWith("FORCE_INDEX", StringComparison.InvariantCultureIgnoreCase))
							{
								tableHintFragments.AddFragment(fragment);
							}
							else
							{
								joinHintFragments.AddFragment(fragment);
							}
							break;
					}
				}
				if(tableHintFragments.Count > 0)
				{
					tableHintFragment = tableHintFragments.ToString();
				}
				if(joinHintFragments.Count > 0)
				{
					joinHintFragment = joinHintFragments.ToString();
				}
			}
        }


		/// <inheritdoc />
		/// We're using temporal predicates as a container to get the join hints across. This is done to avoid breaking interfaces. 
		public override void AppendJoinSideToJoinFragments(QueryFragments toAppendTo, string elementName, string alias, string tableHint, string joinHint)
		{
			if(!string.IsNullOrEmpty(joinHint))
			{
				toAppendTo.AddFragment(joinHint);
			}
			toAppendTo.AddFragment(elementName);
			if(!string.IsNullOrEmpty(tableHint))
			{
				toAppendTo.AddFragment(tableHint);
			}
			if(!string.IsNullOrEmpty(alias))
			{
				toAppendTo.AddFragment(alias);
			}
		}


		/// <summary>
        /// Converts the passed in expression operator (exop) to a string usable in a query 
        /// </summary>
        /// <param name="operatorToConvert">Expression operator to convert to a string</param>
        /// <returns>The string representation usable in a query of the operator passed in.</returns>
        public override string ConvertExpressionOperator(ExOp operatorToConvert)
        {
            string toReturn;
            switch(operatorToConvert)
            {
				case ExOp.Mod:
					throw new ORMQueryConstructionException("The MOD operator isn't support by Google Cloud Spanner. Use a function mapping instead to use the MOD(x,y) function");
                default:
                    toReturn = base.ConvertExpressionOperator(operatorToConvert);
                    break;
            }
            return toReturn;
        }


        /// <summary>
        /// Strips the object name chars from the name passed in. For example [name] will become name
        /// </summary>
        /// <param name="toStrip">To strip.</param>
        /// <returns>
        /// name without the name's object name chars (Which are db specific)
        /// </returns>
        public override string StripObjectNameChars(string toStrip)
        {
            string toMatch = toStrip;

            if(toStrip.StartsWith("`"))
            {
                toMatch = toStrip.Substring(1, toStrip.Length - 2);
            }

            return toMatch;
        }


        /// <summary>
        /// Creates a new dynamic query engine instance
        /// </summary>
        /// <returns></returns>
        protected override DynamicQueryEngineBase CreateDynamicQueryEngine()
        {
            return new DynamicQueryEngine();
        }


        /// <summary>
        /// Sets the ADO.NET provider specific Enum type of the parameter, using the string presentation specified.
        /// </summary>
        /// <param name="parameter">The parameter.</param>
        /// <param name="parameterType">Type of the parameter as string.</param>
        protected override void SetParameterType(DbParameter parameter, string parameterType)
        {
            if(!string.IsNullOrEmpty(parameterType))
            {
                _dbProviderFactoryInfo.SetParameterType(parameter, parameterType);
            }
        }


        /// <summary>
        /// Constructs a call to the aggregate function specified with the field name specified as parameter.
        /// </summary>
        /// <param name="function">The function.</param>
        /// <param name="fieldName">Name of the field.</param>
        /// <returns>
        /// ready to append string which represents the call to the aggregate function with the field as a parameter or the fieldname itself if
        /// the aggregate function isn't known.
        /// </returns>
        /// <remarks>Override this method and replace function with a function which is supported if your database doesn't support the function used.</remarks>
        protected override string ConstructCallToAggregateWithFieldAsParameter(AggregateFunction function, string fieldName)
        {
            AggregateFunction toUse = function;
            switch(toUse)
            {
                case AggregateFunction.CountBig:
                    toUse = AggregateFunction.Count;
                    break;
                case AggregateFunction.CountBigDistinct:
                    toUse = AggregateFunction.CountDistinct;
                    break;
                case AggregateFunction.CountBigRow:
                    toUse = AggregateFunction.CountRow;
                    break;
            }
            return base.ConstructCallToAggregateWithFieldAsParameter(toUse, fieldName);
        }


		/// <inheritdoc />
		protected override void SetParameterSize(DbParameter parameter, string parameterType, int sizeToSet)
		{
			switch(parameterType)
			{
				case "String":
				case "Bytes:":
					base.SetParameterSize(parameter, parameterType, sizeToSet);
					break;
				// rest: ignore, otherwise we'll get an exceptin.
			}
		}


		/// <inheritdoc />
		public override string GetUnionOperatorString(UnionOperatorType operatorToConvert)
		{
			var operatorAsString = string.Empty;
			switch(operatorToConvert)
			{
				case UnionOperatorType.Union:
					operatorAsString = "UNION DISTINCT";
					break;
				case UnionOperatorType.UnionAll:
					operatorAsString = "UNION ALL";
					break;
			}
			return operatorAsString;
		}


		#region Class Property Declarations
        /// <summary>
        /// Gets the DbProviderFactory instance to use.
        /// </summary>
        public override DbProviderFactory FactoryToUse
        {
            get { return _dbProviderFactoryInfo.FactoryToUse; }
        }

        /// <summary>
        /// Gets the parameter prefix, if required. If no parameter prefix is required, this property will return the empty string (by default it returns the empty string).
        /// </summary>
        protected override string ParameterPrefix
        {
            get { return "@"; }
        }

        /// <summary>
        /// Returns the factory to use for reflection, which is the unwrapped factory
        /// </summary>
		internal DbProviderFactory FactoryForReflection
		{
			get { return _dbProviderFactoryInfo.FactoryForReflection; }
		}
        #endregion
    }
}
