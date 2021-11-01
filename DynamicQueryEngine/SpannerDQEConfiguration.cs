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
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SD.LLBLGen.Pro.ORMSupportClasses;

namespace SD.LLBLGen.Pro.DQE.Spanner
{
	/// <summary>
	/// Configuration class for the Google Cloud Spanner DQE
	/// </summary>
	public class SpannerDQEConfiguration : DQEConfigurationBase
	{
		#region Members
		private TraceLevel? _traceLevel;
		#endregion


		/// <summary>
		/// Sets the trace level of the TraceSwitch of the DQE to the level specified. 
		/// </summary>
		/// <param name="level"></param>
		/// <returns></returns>
		public SpannerDQEConfiguration SetTraceLevel(TraceLevel level)
		{
			_traceLevel = level;
			return this;
		}


		/// <summary>
		/// Adds the DbProviderFactory specified as the factory to use.
		/// </summary>
		/// <param name="factoryType">The DbProviderFactory derived type to use</param>
		/// <returns></returns>
		/// <remarks>For Google Cloud Spanner the factory type always has to be registered through this method, for .NET full and .NET standard/core.</remarks>
		public SpannerDQEConfiguration AddDbProviderFactory(Type factoryType)
		{
			StoreDbProviderFactoryRegistration(factoryType, "Google.Cloud.Spanner.Data");
			return this;
		}

        /// <inheritdoc />
        protected override void Configure()
		{
			DynamicQueryEngine.Configure(this);
		}


		#region Properties
		internal TraceLevel? TraceLevel
		{
			get { return _traceLevel; }
		}

		internal Dictionary<string, DbProviderFactory> DbProviderFactories
		{
			get { return this.DbProviderFactoryValues; }
		}
        #endregion
    }
}
