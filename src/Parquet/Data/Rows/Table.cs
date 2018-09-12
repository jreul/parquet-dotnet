using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data.Rows
{
   /// <summary>
   /// Represents a table or table chunk that stores data in row format
   /// </summary>
   public class Table : ICollection<Row>
   {
      internal readonly Dictionary<string, IList> _fieldPathToValues = new Dictionary<string, IList>();

      /// <summary>
      /// Creates an empty table with specified schema
      /// </summary>
      /// <param name="schema"></param>
      public Table(Schema schema)
      {
         Schema = schema ?? throw new ArgumentNullException(nameof(schema));
      }

      /// <summary>
      /// Table schema
      /// </summary>
      public Schema Schema { get; }

      private void AddRow(Row row)
      {
         RowMatrix.Append(this, row);

         Count += 1;
      }

      internal DataColumn[] ExtractDataColunns()
      {
         throw new NotImplementedException();
      }

      #region [ ICollection members ]

      /// <summary>
      /// Number of rows in this table
      /// </summary>
      public int Count { get; private set; }

      /// <summary>
      /// It's not read only!
      /// </summary>
      public bool IsReadOnly => false;

      /// <summary>
      /// Adds a new row, expanding hierarchy when needed
      /// </summary>
      /// <param name="row"></param>
      public void Add(Row row)
      {
         if (row == null)
         {
            throw new ArgumentNullException(nameof(row));
         }

         AddRow(row);
      }

      /// <summary>
      /// 
      /// </summary>
      public void Clear()
      {
         throw new NotImplementedException();
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="item"></param>
      /// <returns></returns>
      public bool Contains(Row item)
      {
         throw new NotImplementedException();
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="array"></param>
      /// <param name="arrayIndex"></param>
      public void CopyTo(Row[] array, int arrayIndex)
      {
         throw new NotImplementedException();
      }

      /// <summary>
      /// 
      /// </summary>
      /// <returns></returns>
      public IEnumerator<Row> GetEnumerator()
      {
         throw new NotImplementedException();
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="item"></param>
      /// <returns></returns>
      public bool Remove(Row item)
      {
         throw new NotImplementedException();
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         throw new NotImplementedException();
      }

      #endregion
   }
}