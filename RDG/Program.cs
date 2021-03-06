﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

namespace RDG
{
    class Program
    {
        private static List<string> commandList;
        private static string path = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), @"GenSource\");

        private static string schema;
        private static readonly DateTime start = new DateTime(1995, 1, 1);
        private static readonly int range = (DateTime.Today - start).Days;

        private static Random rnd = new Random();       

        private static List<string> usedRecords = new List<string>();

        private static Dictionary<string, List<string>> pkDict = new Dictionary<string, List<string>>();

        private static Dictionary<string, List<string>> unusedPkDict = new Dictionary<string, List<string>>();

        private static Dictionary<string, List<string>> genValDict = new Dictionary<string, List<string>>();

        private static void LoadLists()
        {
            genValDict.Add("COUNTRY", File.ReadAllLines(Path.Combine(path,"countries.txt")).Select(x => x.Split(',')[1]).ToList());
            genValDict.Add("CITY", File.ReadAllLines(Path.Combine(path, "cities.txt")).ToList());
            genValDict.Add("NAME", File.ReadAllLines(Path.Combine(path, "firstnames.txt")).ToList());
            genValDict.Add("SURNAME", File.ReadAllLines(Path.Combine(path, "lastnames.txt")).ToList());
            genValDict.Add("LANGUAGE", File.ReadAllLines(Path.Combine(path, "languages.txt")).ToList());
            genValDict.Add("COMPANY", File.ReadAllLines(Path.Combine(path, "company.txt")).ToList());
            genValDict.Add("SEX", new List<string> { "MALE", "FEMALE" });
            genValDict.Add("USERNAME", File.ReadAllLines(Path.Combine(path, "usernames.txt")).ToList());
            genValDict.Add("TITLE", File.ReadAllLines(Path.Combine(path, "titles.txt")).ToList());
        }

        private static string GetRandomFromList(List<string> list, bool addNum = false)
        {
            if(!addNum)
                return list[rnd.Next(0, list.Count)];
            else
                return list[rnd.Next(0, list.Count)] + rnd.Next(1000, 9999);
        }

        private static string GetUniqueRandomFromList(List<string> list)
        {
            List<string> unused = list.Except(usedRecords).ToList();
            string selected = unused[rnd.Next(0, unused.Count)];
            usedRecords.Add(selected);
            return selected;
        }

        private static string GetUniqueRandomFromPKList(String name)
        {
            string selected = unusedPkDict[name][rnd.Next(0, unusedPkDict[name].Count)];
            unusedPkDict[name].Remove(selected);
            return selected;
        }

        private static string generateInsert(string table, List<string> values)
        {
            Regex regex = new Regex(@"\d\d\.\d\d\.\d\d\d\d");
            string output = "INSERT INTO " + schema + "." + table+" VALUES(";

            int dummy;
            for(int i=0;i<values.Count-1;i++)
            {
                if (values[i].Equals("NULL"))
                    output += "NULL,";
                else if (Int32.TryParse(values[i], out dummy))
                    output += values[i] + ",";
                else if (regex.Match(values[i]).Success)
                    output +="to_date('" + values[i] + "','DD.MM.YYYY'),";
                else
                    output += "'" + values[i] + "',";
            }
            if (values.Last().Equals("NULL"))
                output += "NULL);";
            else if (Int32.TryParse(values.Last(), out dummy))
                output += values.Last() + ");";
            else if (regex.Match(values.Last()).Success)
                output += "to_date('" + values.Last() + "','DD.MM.YYYY'));";
            else
                output += "'" + values.Last() + "');";

            return output;
        }

        private static string getRandomString(int length)
        {
            string chars = "abcdefghijklmnopqrstuvwxyz";
            string output = "";
            for (int i = 0; i < length; i++)
            {
                output += chars.ToCharArray()[rnd.Next(0, chars.Length)].ToString();
            }
            return output;
        }

        private static string getRandomDate()
        {
            return start.AddDays(rnd.Next(range)).ToString("dd/MM/yyyy");
        }

        private static void LoadCommandFile()
        {
            commandList = File.ReadAllLines(Path.Combine(path, "commands.txt")).Where(x=>!x.StartsWith("#") && !x.Equals("")).ToList();

            if (commandList[0].Split('=')[1] == null)
            {
                throw new Exception();
            }
            else
            {
                schema = commandList[0].Split('=')[1];
                commandList.RemoveAt(0);
            }
        }

        private static void ExecuteCommands()
        {
            List<string> allLines = new List<string>();
            string[] commandParts;

            List<string> values = new List<string>();
            List<string> pkList = new List<string>();

            string tablename;
            int recCount;

            foreach (string command in commandList)
            {
                unusedPkDict = new Dictionary<string, List<string>>();
                foreach (KeyValuePair<string,List<string>> list in pkDict)
                {
                    unusedPkDict.Add(list.Key, new List<string>(list.Value));
                }
                //unusedPkDict = new Dictionary<string, List<string>>(pkDict);
                usedRecords = new List<string>();
                commandParts = command.Split(',');
                tablename = commandParts[0];
                pkList = new List<string>();
                int idCounter = 0;

                if (!Int32.TryParse(commandParts[1],out recCount))
                    throw new Exception();
                for (int i = 0; i < recCount; i++)
                {
                    values = new List<string>();
                    for (int j = 2; j < commandParts.Length; j++)
                    {
                        string[] insertInfo = commandParts[j].Split('/');
                        string outputValue = "";
                        switch (insertInfo[0])
                        {
                            case "S":
                                string[] strGenDetail = insertInfo[1].Split('|');
                                string[] strGenType = insertInfo[2].Split('|');
                                int lenInt;
                                int lenInt2;
                                if (Int32.TryParse(strGenType[0], out lenInt))
                                {
                                    if (Int32.TryParse(strGenType[1], out lenInt2))
                                        outputValue = getRandomString(rnd.Next(lenInt,lenInt2));
                                    else
                                        outputValue = getRandomString(lenInt);
                                }
                                else
                                {
                                    if (strGenType.Length>1 && strGenType[1].Equals("PK", StringComparison.OrdinalIgnoreCase))
                                    {
                                        if (strGenDetail[0] == "")
                                            outputValue = GetRandomFromList(pkDict[strGenType[0]]);
                                        if (strGenDetail[0] == "U")
                                            outputValue = GetUniqueRandomFromPKList(strGenType[0]);
                                    }
                                    else
                                    {
                                        if (strGenDetail[0] == "")
                                            if (strGenType[0].Equals("USERNAME", StringComparison.OrdinalIgnoreCase))
                                                outputValue = GetRandomFromList(genValDict[strGenType[0]], true);
                                            else if (strGenType[0].Equals("CITY", StringComparison.OrdinalIgnoreCase))
                                                outputValue = GetRandomFromList(genValDict[strGenType[0]]) + getRandomString(4);
                                            else
                                                outputValue = GetRandomFromList(genValDict[strGenType[0]]);
                                        if (strGenDetail[0] == "U")
                                            outputValue = GetUniqueRandomFromList(genValDict[strGenType[0]]);
                                    }
                                    if (strGenDetail.Length > 1)
                                    {
                                        if (Int32.TryParse(strGenDetail[1], out lenInt))
                                        {
                                            outputValue = (rnd.Next(0, 100) > lenInt) ? outputValue : "NULL";
                                        }
                                    }
                                }
                                break;
                            case "N":
                                if (insertInfo[1].Equals("ID", StringComparison.OrdinalIgnoreCase))
                                    outputValue = idCounter++.ToString();
                                else
                                {
                                    string[] rndNumRange = insertInfo[2].Split('|');
                                    outputValue = rnd.Next(Int32.Parse(rndNumRange[0]), Int32.Parse(rndNumRange[1])).ToString();
                                }
                                break;
                            case "D":
                                outputValue = getRandomDate();
                                break;
                            default:
                                throw new Exception();
                        }
                        values.Add(outputValue);
                        if (j == 2)
                            pkList.Add(outputValue);
                    }

                    allLines.Add(generateInsert(tablename, values));
                }
                try
                {
                    pkDict.Add(tablename, new List<string>(pkList));
                }
                catch (ArgumentException ex)
                {
                    pkDict[tablename] = new List<string>(pkDict[tablename].Concat(pkList).ToList());
                }
                allLines.Add("--===========================================================================");
                Console.WriteLine("DONE " + tablename);
            }
            File.WriteAllLines("insert.sql", allLines);
        }

        static void Main(string[] args)
        {
            LoadLists();
            LoadCommandFile();
            ExecuteCommands();
        }
    }
}
