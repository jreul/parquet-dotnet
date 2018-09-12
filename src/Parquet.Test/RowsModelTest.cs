using System.IO;
using Parquet.Data;
using Parquet.Data.Rows;
using Xunit;

namespace Parquet.Test
{
   public class RowsModelTest
   {
      [Fact]
      public void Read_write_flat_table()
      {
         var table = new Table(new Schema(new DataField<int>("id"), new DataField<string>("city")));
         var ms = new MemoryStream();

         //generate fake data
         for(int i = 0; i < 1000; i++)
         {
            table.Add(new Row(i, "record#" + i));
         }

         //write to stream
         using (var writer = new ParquetWriter(table.Schema, ms))
         {
            writer.Write(table);
         }
         ms.Position = 0;

         //read back into table

      }
   }
}