////////////////////////////////////////////////////////////////////////////////////////////
// Implementation of the Google Cloud Spanner DBDriver for the LLBLGen Pro system.
// (c) 2002-2019 Solutions Design, all rights reserved.
// http://www.llblgen.com/
// 
// THIS IS NOT OPEN SOURCE SOFTWARE OF ANY KIND. 
// 
// Designed and developed by Frans Bouma.
///////////////////////////////////////////////////////////////////////////////////////////
namespace SD.LLBLGen.Pro.DBDrivers.Spanner
{
	/// <summary>
	/// List of types Google Cloud SPanner supports. 
	/// </summary>
	public enum SpannerDbTypes
	{
		/// <summary>
		/// 
		/// </summary>
		Bool,
		Bytes,
		Date,
		Float64,
		Int64,
		String,
		Timestamp,
		// add more here
		NumberOfSqlDbTypes
	}
}
