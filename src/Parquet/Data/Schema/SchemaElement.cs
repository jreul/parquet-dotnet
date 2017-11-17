﻿using System;

namespace Parquet.Data
{

   /// <summary>
   /// Element of dataset's schema
   /// </summary>
   public class SchemaElement : IEquatable<SchemaElement>
   {
      /// <summary>
      /// Data type of this element
      /// </summary>
      public DataType DataType { get; }

      /// <summary>
      /// When true, this element is allowed to have nulls
      /// </summary>
      public bool HasNulls { get; }

      /// <summary>
      /// When true, the value is an array rather than a single value.
      /// </summary>
      public bool IsArray { get; }

      /// <summary>
      /// Column name
      /// </summary>
      public string Name { get; private set; }

      /// <summary>
      /// CLR type of this column. Not sure whether to expose this externally yet.
      /// </summary>
      internal Type ClrType { get; private set; }

      /// <summary>
      /// Only used internally!
      /// </summary>
      internal string Path { get; set; }

      public SchemaElement(string name, DataType dataType, bool hasNulls = true, bool isArray = false)
      {
         Name = name ?? throw new ArgumentNullException(nameof(name));

         DataType = dataType;
         HasNulls = hasNulls;
         IsArray = isArray;

         IDataTypeHandler handler = DataTypeFactory.Match(dataType);
         if (handler != null)
         {
            ClrType = handler.ClrType;
         }
      }

      /// <summary>
      /// Pretty prints
      /// </summary>
      public override string ToString()
      {
         return $"{Name}: {DataType} (HN: {HasNulls}, IA: {IsArray})";
      }

      /// <summary>
      /// Indicates whether the current object is equal to another object of the same type.
      /// </summary>
      /// <param name="other">An object to compare with this object.</param>
      /// <returns>
      /// true if the current object is equal to the <paramref name="other" /> parameter; otherwise, false.
      /// </returns>
      public bool Equals(SchemaElement other)
      {
         if (ReferenceEquals(null, other)) return false;
         if (ReferenceEquals(this, other)) return true;

         //todo: check equality for child elements

         return
            string.Equals(Name, other.Name) &&
            DataType.Equals(other.DataType) &&
            HasNulls == other.HasNulls &&
            IsArray == other.IsArray;
      }

      /// <summary>
      /// Determines whether the specified <see cref="System.Object" />, is equal to this instance.
      /// </summary>
      /// <param name="obj">The <see cref="System.Object" /> to compare with this instance.</param>
      /// <returns>
      ///   <c>true</c> if the specified <see cref="System.Object" /> is equal to this instance; otherwise, <c>false</c>.
      /// </returns>
      public override bool Equals(object obj)
      {
         if (ReferenceEquals(null, obj)) return false;
         if (ReferenceEquals(this, obj)) return true;
         if (obj.GetType() != GetType()) return false;

         return Equals((SchemaElement) obj);
      }

      /// <summary>
      /// Returns a hash code for this instance.
      /// </summary>
      /// <returns>
      /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table.
      /// </returns>
      public override int GetHashCode()
      {
         return Name.GetHashCode() * DataType.GetHashCode();
      }

      #region [ Internal Helpers ]

      internal virtual void Assign(SchemaElement se)
      {

      }

      #endregion
   }
}