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
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Google.Cloud.Spanner.Data;
using SD.LLBLGen.Pro.DBDriverCore;

namespace SD.LLBLGen.Pro.DBDrivers.Spanner
{
	/// <summary>
	/// Specific strategy to be used with Google Cloud Spanner 
	/// </summary>
	public class SpannerRecoveryStrategy : RecoveryStrategyBase
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="SpannerRecoveryStrategy"/> class.
		/// </summary>
		public SpannerRecoveryStrategy() : base()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="SpannerRecoveryStrategy"/> class.
		/// </summary>
		/// <param name="maximumNumberOfRetries">The maximum number of retries.</param>
		/// <param name="delayCalculator">The delay calculator.</param>
		public SpannerRecoveryStrategy(int maximumNumberOfRetries, RecoveryDelay delayCalculator) : base(maximumNumberOfRetries, delayCalculator)
		{
		}


		/// <summary>
		/// Determines whether the specified exception is a transient exception.
		/// </summary>
		/// <param name="toCheck">The exception to check.</param>
		/// <returns>
		/// true if the exception is a transient exception and can be retried, false otherwise. The empty implementation
		/// returns false.
		/// </returns>
		protected override bool IsTransientException(Exception toCheck)
		{
			if(toCheck is TimeoutException)
			{
				return true;
			}
			var toCheckAsSpannerException = toCheck as SpannerException;
			if(toCheckAsSpannerException == null)
			{
				return false;
			}

			return toCheckAsSpannerException.IsTransientSpannerFault();
		}
	}
}
