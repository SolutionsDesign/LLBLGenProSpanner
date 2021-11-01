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
using System.ComponentModel;
using SD.LLBLGen.Pro.DBDriverCore;
using SD.Tools.BCLExtensions.CollectionsRelated;

namespace SD.LLBLGen.Pro.DBDrivers.Spanner
{
	/// <summary>
	/// Data collector for the retrieval of connection information for Google Cloud Spanner style connection strings.
	/// </summary>
	public class SpannerConnectionData : ConnectionDataBase
	{
		#region Members
		private string _projectID, _instanceID, _databaseID;
		private bool _useTransientErrorRecovery;
		#endregion

		/// <summary>
		/// CTor
		/// </summary>
		/// <param name="driverToUse">The driver instance to use to retrieve connection information.</param>
		public SpannerConnectionData(SpannerDBDriver driverToUse)
			: base(driverToUse)
		{
		}


		/// <summary>
		/// Fills the properties of this object with the data obtained from the ConnectionElements object available in the set driver (if any)
		/// </summary>
		public override void FillProperties()
		{
			if(this.ConnectionElements.Count <= 0)
			{
				return;
			}

			_projectID = this.ConnectionElements.GetValue(ConnectionElement.ProjectID) ?? string.Empty;;
			_instanceID = this.ConnectionElements.GetValue(ConnectionElement.InstanceID) ?? string.Empty;;
			_databaseID = this.ConnectionElements.GetValue(ConnectionElement.CatalogName) ?? string.Empty;;
			_useTransientErrorRecovery = this.ConnectionElements.ContainsKey(ConnectionElement.UseTransientErrorRecovery);
		}


		/// <summary>
		/// Validates the information specified. 
		/// </summary>
		/// <returns>returns true if the information is valid, false otherwise. Caller should not proceed further if false is returned.</returns>
		public override bool ValidateInformation()
		{
			return !string.IsNullOrWhiteSpace(_projectID) && !string.IsNullOrWhiteSpace(_instanceID) && !string.IsNullOrWhiteSpace(_instanceID);
		}


		/// <summary>
		/// Constructs a dictionary with the elements for the connection string
		/// </summary>
		protected override void ConstructConnectionElementsList()
		{
			this.ConnectionElements.Clear();

			this.ConnectionElements.Add(ConnectionElement.ProjectID, _projectID);
			this.ConnectionElements.Add(ConnectionElement.InstanceID, _instanceID);
			this.ConnectionElements.Add(ConnectionElement.CatalogName, _databaseID);
			if(_useTransientErrorRecovery)
			{
				this.ConnectionElements.Add(ConnectionElement.UseTransientErrorRecovery, "true");
			}
		}


		/// <summary>
		/// Gets the property names for the property bag to bind to the property grid. This list of names is used to create property descriptors
		/// which are easier to read as they have their names split on word breaks.
		/// </summary>
		/// <returns></returns>
		protected override HashSet<string> GetPropertyNamesForBag()
		{
			return new HashSet<string>() { "ProjectID", "InstanceID",  "DatabaseID", "UseTransientErrorRecovery" };
		}


		#region Properties
		/// <summary>
		/// Gets or sets the project id
		/// </summary>
		[Description("The ProjectID to connect to.")]
		[Category("General")]
		public string ProjectID
		{
			get { return _projectID; }
			set
			{
				var oldValue = _projectID;
				_projectID= value;
				RaiseOnChange(oldValue, value, "ProjectID");
			}
		}


		/// <summary>
		/// Gets or sets a value indicating whether [use transient error recovery].
		/// </summary>
		[Description("When set to true, will use transient error recovery when a transient error occurs during meta-data retrieval. Recommended to set to true when you run into deadlocks while connecting to Google Cloud Spanner")]
		[Category("Error handling")]
		public bool UseTransientErrorRecovery
		{
			get { return _useTransientErrorRecovery; }
			set
			{
				var oldValue = _useTransientErrorRecovery;
				_useTransientErrorRecovery = value;
				RaiseOnChange(oldValue, value, "UseTransientErrorRecovery");
			}
		}


		/// <summary>
		/// Gets or sets the instance ID
		/// </summary>
		[Description("The Instance ID to connect to")]
		[Category("General")]
		public string InstanceID
		{
			get { return _instanceID; }
			set
			{
				var oldValue = _instanceID;
				_instanceID = value;
				RaiseOnChange(oldValue, value, "InstanceID");
			}
		}


		/// <summary>
		/// Gets or sets the database id
		/// </summary>
		[Description("The Database ID to connect to")]
		[Category("General")]
		public string DatabaseID
		{
			get { return _databaseID; }
			set
			{
				var oldValue = _databaseID;
				_databaseID = value;
				RaiseOnChange(oldValue, value, "DatabaseID");
			}
		}
		#endregion
	}
}
