using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;
using Parquet.Data.Rows;
using Parquet.File;

namespace Parquet
{
   /// <summary>
   /// Defines extension methods to simplify Parquet usage (experimental v3)
   /// </summary>
   public static class ParquetExtensions
   {
      /// <summary>
      /// Writes a file with a single row group
      /// </summary>
      public static void WriteSingleRowGroupParquetFile(this Stream stream, Schema schema, int rowCount, params DataColumn[] columns)
      {
         using (var writer = new ParquetWriter(schema, stream))
         {
            writer.CompressionMethod = CompressionMethod.None;
            using (ParquetRowGroupWriter rgw = writer.CreateRowGroup(rowCount))
            {
               foreach(DataColumn column in columns)
               {
                  rgw.WriteColumn(column);
               }
            }
         }
      }

      /// <summary>
      /// Reads the first row group from a file
      /// </summary>
      /// <param name="stream"></param>
      /// <param name="schema"></param>
      /// <param name="columns"></param>
      public static void ReadSingleRowGroupParquetFile(this Stream stream, out Schema schema, out DataColumn[] columns)
      {
         using (var reader = new ParquetReader(stream))
         {
            schema = reader.Schema;

            using (ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0))
            {
               DataField[] dataFields = schema.GetDataFields();
               columns = new DataColumn[dataFields.Length];

               for(int i = 0; i < dataFields.Length; i++)
               {
                  columns[i] = rgr.ReadColumn(dataFields[i]);
               }
            }
         }
      }

      /// <summary>
      /// Writes entire table in a single row group
      /// </summary>
      /// <param name="writer"></param>
      /// <param name="table"></param>
      public static void Write(this ParquetWriter writer, Table table)
      {
         using (ParquetRowGroupWriter rowGroupWriter = writer.CreateRowGroup(table.Count))
         {
            rowGroupWriter.Write(table);
         }
      }

      /// <summary>
      /// Writes table to this row group
      /// </summary>
      /// <param name="writer"></param>
      /// <param name="table"></param>
      public static void Write(this ParquetRowGroupWriter writer, Table table)
      {
         DataColumn[] columns = table.ExtractDataColunns();
         for (int i = 0; i < columns.Length; i++)
         {
            writer.WriteColumn(columns[i]);
         }
      }
   }
}