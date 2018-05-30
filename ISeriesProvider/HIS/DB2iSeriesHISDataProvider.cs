using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using Data;
	using Extensions;
	
	public class DB2iSeriesHISDataProvider : DB2iSeriesDataProvider
	{
        public DB2iSeriesHISDataProvider() : this(DB2iSeriesProviderName.DB2, DB2iSeriesLevels.Any, false)
        {
        }

        public DB2iSeriesHISDataProvider(string name, DB2iSeriesLevels minLevel, bool mapGuidAsString) : base(name, minLevel, mapGuidAsString)
        {
            
        }
       
		#region "overrides"

		public override string ConnectionNamespace { get { return ""; } }
		protected override string ConnectionTypeName { get { return DB2iSeriesHISTools.ConnectionTypeName; } }
		protected override string DataReaderTypeName { get { return DB2iSeriesHISTools.DataReaderTypeName; } }
		
		protected override void OnConnectionTypeCreated(Type connectionType)
		{
            DB2iSeriesHISTypes.ConnectionType = connectionType;
            
            if (DataConnection.TraceSwitch.TraceInfo)
            {
                DataConnection.WriteTraceLine(
                    DataReaderType.AssemblyEx().FullName,
                    DataConnection.TraceSwitch.DisplayName);

                DataConnection.WriteTraceLine(
                    DB2iSeriesHISTypes.DB2DateTime.IsSupported ? "DB2DateTime is supported." : "DB2DateTime is not supported.",
                    DataConnection.TraceSwitch.DisplayName);
            }
            DB2iSeriesDB2ConnectTools.Initialized();
		}

		#endregion
	}
}