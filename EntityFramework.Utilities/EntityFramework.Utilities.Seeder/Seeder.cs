using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Migrations;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using CsvHelper;
using CsvHelper.TypeConversion;
using System.Threading;
using System.Globalization;

namespace EntityFramework.Utilities.Seeder
{
    /// <summary>
    /// A set of helper methods for seeding dbContexts from CSV files
    /// </summary>
    public static class Seeder
    {
        private static IList<T> GetEntitiesFromStream<T>(Stream stream, params CsvColumnMapping<T>[] additionalMapping) where T : class
        {
            IList<T> entities = new List<T>();
            using (StreamReader reader = new StreamReader(stream))
            {
                CsvReader csvReader = new CsvReader(reader);
                var map = csvReader.Configuration.AutoMap<T>();
                map.ReferenceMaps.Clear();
                csvReader.Configuration.RegisterClassMap(map);
                csvReader.Configuration.WillThrowOnMissingField = false;
                
                var currentCulture = Thread.CurrentThread.CurrentCulture;
                Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;

                while (csvReader.Read())
                {
                    T entity = null;
                    try
                    {
                        entity = csvReader.GetRecord<T>();
                    }
                    catch (CsvTypeConverterException ex)
                    {
                        throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Error parsing file"), ex);
                    }

                    foreach (CsvColumnMapping<T> csvColumnMapping in additionalMapping)
                    {
                        csvColumnMapping.Execute(entity, csvReader.GetField(csvColumnMapping.CsvColumnName));
                    }
                    entities.Add(entity);
                }

                Thread.CurrentThread.CurrentCulture = currentCulture;
            }

            return entities;
        }


        /// <summary>
        /// Seeds a dbContext from a CSV file that will be read from the specified stream
        /// </summary>
        /// <typeparam name="T">The type of entity to load</typeparam>
        /// <param name="dbContext">The dbContext to populate</param>
        /// <param name="stream">The stream containing the CSV file contents</param>
        /// <param name="identifierExpression">An expression specifying the properties that should be used when determining whether an Add or Update operation should be performed.</param>
        /// <param name="additionalMapping">Any additonal complex mappings required</param>
        public static void SeedFromStream<T>(this DbContext dbContext, Stream stream, Expression<Func<T, object>> identifierExpression, params CsvColumnMapping<T>[] additionalMapping) where T : class
        {
            foreach (T entity in GetEntitiesFromStream(stream, additionalMapping))
            {
                dbContext.Set<T>().AddOrUpdate(identifierExpression, entity);
            }
        }

        /// <summary>
        /// Seeds a dbContext from a CSV file that will be read from the specified stream using BulkInsert operation
        /// </summary>
        /// <typeparam name="T">The type of entity to load</typeparam>
        /// <param name="dbContext">The dbContext to populate</param>
        /// <param name="stream">The stream containing the CSV file contents</param>
        /// <param name="additionalMapping">Any additonal complex mappings required</param>
        public static void SeedFromStreamWithBulkInsert<T>(this DbContext dbContext, Stream stream, params CsvColumnMapping<T>[] additionalMapping) where T : class
        {
            EFBatchOperation.For(dbContext, dbContext.Set<T>()).InsertAll(GetEntitiesFromStream(stream, additionalMapping));
        }


        /// <summary>
        /// Seeds a dbContext from a CSV file that will be read from the specified embedded resource
        /// </summary>
        /// <typeparam name="T">The type of entity to load</typeparam>
        /// <param name="dbContext">The dbContext to populate</param>
        /// <param name="embeddedResourceName">The name of the embedded resource containing the CSV file contents</param>
        /// <param name="identifierExpression">An expression specifying the properties that should be used when determining whether an Add or Update operation should be performed.</param>
        /// <param name="additionalMapping">Any additonal complex mappings required</param>
        public static void SeedFromResource<T>(this DbContext dbContext, string embeddedResourceName, Expression<Func<T, object>> identifierExpression, params CsvColumnMapping<T>[] additionalMapping) where T : class
        {
            Assembly assembly = Assembly.GetCallingAssembly();
            using (Stream stream = assembly.GetManifestResourceStream(embeddedResourceName))
            {
                SeedFromStream(dbContext, stream, identifierExpression, additionalMapping);
            }
        }


        /// <summary>
        /// Seeds a dbContext from a CSV file that will be read from the specified embedded resource using BulkInsert operation
        /// </summary>
        /// <typeparam name="T">The type of entity to load</typeparam>
        /// <param name="dbContext">The dbContext to populate</param>
        /// <param name="embeddedResourceName">The name of the embedded resource containing the CSV file contents</param>
        /// <param name="additionalMapping">Any additonal complex mappings required</param>
        public static void SeedFromResourceWithBulkInsert<T>(this DbContext dbContext, string embeddedResourceName, params CsvColumnMapping<T>[] additionalMapping) where T : class
        {
            Assembly assembly = Assembly.GetCallingAssembly();
            using (Stream stream = assembly.GetManifestResourceStream(embeddedResourceName))
            {
                SeedFromStreamWithBulkInsert(dbContext, stream, additionalMapping);
            }
        }


        /// <summary>
        /// Seeds a dbContext from a CSV file that will be read from the specified file name
        /// </summary>
        /// <typeparam name="T">The type of entity to load</typeparam>
        /// <param name="dbContext">The dbContext to populate</param>
        /// <param name="fileName">The name of the file containing the CSV file contents</param>
        /// <param name="identifierExpression">An expression specifying the properties that should be used when determining whether an Add or Update operation should be performed.</param>
        /// <param name="additionalMapping">Any additonal complex mappings required</param>
        public static void SeedFromFile<T>(this DbContext dbContext, string fileName, Expression<Func<T, object>> identifierExpression, params CsvColumnMapping<T>[] additionalMapping) where T : class
        {
            using (Stream stream = File.OpenRead(fileName))
            {
                SeedFromStream(dbContext, stream, identifierExpression, additionalMapping);
            }
        }


        /// <summary>
        /// Seeds a dbContext from a CSV file that will be read from the specified file name using BulkInsert operation
        /// </summary>
        /// <typeparam name="T">The type of entity to load</typeparam>
        /// <param name="dbContext">The dbContext to populate</param>
        /// <param name="fileName">The name of the file containing the CSV file contents</param>
        /// <param name="additionalMapping">Any additonal complex mappings required</param>
        public static void SeedFromFileWithBulkInsert<T>(this DbContext dbContext, string fileName, params CsvColumnMapping<T>[] additionalMapping) where T : class
        {
            using (Stream stream = File.OpenRead(fileName))
            {
                SeedFromStreamWithBulkInsert(dbContext, stream, additionalMapping);
            }
        }
    }
}
