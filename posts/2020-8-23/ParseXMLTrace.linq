<Query Kind="Statements">
  <NuGetReference>CsvHelper</NuGetReference>
  <Namespace>CsvHelper</Namespace>
  <Namespace>CsvHelper.Configuration</Namespace>
  <Namespace>CsvHelper.Configuration.Attributes</Namespace>
  <Namespace>CsvHelper.Expressions</Namespace>
  <Namespace>CsvHelper.TypeConversion</Namespace>
  <Namespace>System.Globalization</Namespace>
</Query>

string fileName = "Z:\\ByTask\\TW19705447 - VM39 Slowdown\\trace-good.xml";
string sqlSearch = "133730";


// Begin Main process...
XElement loadedXML = XElement.Load(fileName);

// Get Server Information
var serverInformationNode = loadedXML.Elements().FirstOrDefault(x => x.Name.LocalName == "Header");
var serverItself = serverInformationNode.Elements().FirstOrDefault(x => x.Name.LocalName == "ServerInformation");

Console.WriteLine($"Processing data from Server: {serverItself.Attribute("name").Value}");

//Start with the Trace itself
List<TraceData> traceData = new List<TraceData>();
TraceData lastTrace = null;

foreach(XElement element in loadedXML.Elements().Where(x => x.Name.LocalName == "Events").Descendants().Where(xx => xx.Elements().Any() 
          && xx.Attribute("name")?.Value == "RPC:Completed").Where(xxx => xxx.Elements().Any(xxxx => xxxx.Attribute("name").Value == "TextData")))
{
	var elements = element.Elements();
	
	TraceData dataToAdd = new TraceData() {
		LoginName = elements.FirstOrDefault(x => x.Attribute("name")?.Value == "NTUserName" || x.Attribute("name")?.Value == "LoginName")?.Value,
		Sql = elements.FirstOrDefault(x => x.Attribute("name")?.Value == "TextData").Value,
		ApplicationName = elements.FirstOrDefault(x => x.Attribute("name")?.Value == "ApplicationName").Value,
	};
	
	if(!string.IsNullOrEmpty(sqlSearch)) {
		if(!dataToAdd.Sql.Contains(sqlSearch))
		{
			continue;
		}
	}
	
	int spid = 0;
	if(int.TryParse(elements.FirstOrDefault(x => x.Attribute("name")?.Value == "SPID")?.Value, out spid))
	{
		dataToAdd.Spid = spid;
	}

	int duration = 0;
	if(int.TryParse(elements.FirstOrDefault(x => x.Attribute("name")?.Value == "Duration")?.Value, out duration))
	{
		dataToAdd.DurationMicro = duration;
	}
	
	int clientId = 0;
	if(int.TryParse(elements.FirstOrDefault(x => x.Attribute("name")?.Value == "ClientProcessID")?.Value, out clientId))
	{
		dataToAdd.ClientProcessId = clientId;
	}
	
	int reads = 0;
	if(int.TryParse(elements.FirstOrDefault(x => x.Attribute("name")?.Value == "Reads")?.Value, out reads)) {
		dataToAdd.Reads = reads;
	}

	int writes = 0;
	if (int.TryParse(elements.FirstOrDefault(x => x.Attribute("name")?.Value == "Writes")?.Value, out writes))
	{
		dataToAdd.Writes = writes;
	}

	int cpu = 0;
	if (int.TryParse(elements.FirstOrDefault(x => x.Attribute("name")?.Value == "CPU")?.Value, out cpu))
	{
		dataToAdd.Cpu = cpu;
	}
	
	DateTime startTime;
	if (DateTime.TryParse(elements.FirstOrDefault(x => x.Attribute("name")?.Value == "StartTime")?.Value, out startTime))
	{
		dataToAdd.StartTimeO = startTime;
	}

	DateTime endTime;
	if (DateTime.TryParse(elements.FirstOrDefault(x => x.Attribute("name")?.Value == "EndTime")?.Value, out endTime))
	{
		dataToAdd.EndTimeO = endTime;
	} else {
		dataToAdd.EndTimeO = dataToAdd.StartTimeO;
	}
	
	if(lastTrace != null) {
		dataToAdd.DeltaFromPreviousO = dataToAdd.StartTimeO - lastTrace.EndTimeO;
	}
	lastTrace = dataToAdd;
	traceData.Add(dataToAdd);
}

string report = $"{Path.GetDirectoryName(fileName)}\\{Path.GetFileNameWithoutExtension(fileName)}.csv";

Console.WriteLine($"Writing report to... {report}");

using (var csvWriter = new StreamWriter(report))
{
	using (var writer = new CsvWriter(csvWriter, CultureInfo.InvariantCulture))
	{
		writer.WriteRecords(traceData);
	}
}

}

internal sealed class TraceData
{
	public string LoginName { get; set; }
	public string ApplicationName { get; set; }
	[Ignore]
	public DateTime StartTimeO { get; set; }
	public string StartDate { get => StartTimeO.ToString("MM/dd/yyyy"); }
	public string StartTime { get => StartTimeO.ToString("H:mm:ss:fff"); }
	[Ignore]
	public DateTime EndTimeO { get; set; }
	public string EndDate { get => EndTimeO.ToString("MM/dd/yyyy"); }
	public string EndTime { get => EndTimeO.ToString("H:mm:ss:fff"); }
	public long DurationMicro { get; set; }
	public long DurationMs { get => DurationMicro / 1024; }
	[Ignore]
	public TimeSpan DeltaFromPreviousO { get; set; }
	public int DeltaFromPreviousMs
	{
		get {
		  return (int) Math.Round(DeltaFromPreviousO.TotalMilliseconds);
		}
	}
	[Ignore]
	public string Sql {get;set;}
	public string SqlShort {get => Sql?.Substring(0,50); }
	public int ClientProcessId {get;set;}
	public int Spid {get; set; }
	public int Reads {get;set;}
	public int Writes {get;set;}
	public int Cpu {get;set;}
