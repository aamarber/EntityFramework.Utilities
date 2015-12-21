using CsvHelper;
using CsvHelper.TypeConversion;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Globalization;
using System.IO;
using System.Linq.Expressions;
using System.Threading;

namespace EntityFramework.Utilities.Seeder
{
    /// <summary>
    /// A set of helper methods for seeding dbContexts from CSV files
    /// </summary>
    public static class Seeder
    {
        public static IList<T> GetEntitiesFromStream<T>(Stream stream, params CsvColumnMapping<T>[] additionalMapping) where T : class
        {
            IList<T> entities = new List<T>();
            using (StreamReader reader = new StreamReader(stream))
            {
                CsvReader csvReader = GetReader<T>(reader);

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
                        throw new InvalidOperationException(
                            string.Format(CultureInfo.InvariantCulture, "Error parsing file"), ex);
                    }
                    catch (FormatException ex)
                    {
                        throw new InvalidOperationException(
                            string.Format(CultureInfo.InvariantCulture, "Error parsing file"), ex);
                    }

                    foreach (CsvColumnMapping<T> csvColumnMapping in additionalMapping)
                    {
                        if (csvColumnMapping.Culture != null)
                        {
                            Thread.CurrentThread.CurrentCulture = csvColumnMapping.Culture;
                            csvReader.Configuration.CultureInfo = csvColumnMapping.Culture;
                        }
                        else
                        {
                            Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
                            csvReader.Configuration.CultureInfo = CultureInfo.InvariantCulture;
                        }

                        csvReader.ClearRecordCache();

                        var record = csvReader.GetRecord<T>();

                        csvColumnMapping.Execute(ref entity, csvReader.GetField(csvColumnMapping.CsvColumnName));
                    }

                    entities.Add(entity);
                }

                Thread.CurrentThread.CurrentCulture = currentCulture;
            }

            return entities;
        }

        private static CsvReader GetReader<T>(StreamReader reader)
        {
            CsvReader csvReader = new CsvReader(reader);
            csvReader.Configuration.CultureInfo = CultureInfo.InvariantCulture;
            var map = csvReader.Configuration.AutoMap<T>();
            map.ReferenceMaps.Clear();
            csvReader.Configuration.RegisterClassMap(map);
            csvReader.Configuration.WillThrowOnMissingField = false;

            return csvReader;
        }

        /// <summary>
        /// Seeds a dbContext from a CSV file that will be read from the specified stream
        /// </summary>
        /// <typeparam name="T">The type of entity to load</typeparam>
        /// <param name="dbContext">The dbContext to populate</param>
        /// <param name="stream">The stream containing the CSV file contents</param>
        /// <param name="identifierExpression">An expression specifying the properties that should be used when determining whether an Add or Update operation should be performed.</param>
        /// <param name="additionalMapping">Any additonal complex mappings required</param>
        /// <returns>Returns the read entities from the CSV resource. <remarks>It does not return the inserted entities</remarks></returns>
        public static IList<T> SeedFromStream<T>(this DbContext dbContext, Stream stream, Expression<Func<T, object>> identifierExpression, params CsvColumnMapping<T>[] additionalMapping) where T : class
        {
            IList<T> entities = GetEntitiesFromStream(stream, additionalMapping);

            dbContext.Set<T>().AddRange(entities);

            return entities;
        }

        /// <summary>
        /// Seeds a dbContext from a CSV file that will be read from the specified stream using BulkInsert operation
        /// </summary>
        /// <typeparam name="T">The type of entity to load</typeparam>
        /// <param name="dbContext">The dbContext to populate</param>
        /// <param name="stream">The stream containing the CSV file contents</param>
        /// <param name="additionalMapping">Any additonal complex mappings required</param>
        /// <returns>Returns the read entities from the CSV resource. <remarks>It does not return the inserted entities</remarks></returns>
        public static IList<T> SeedFromStreamWithBulkInsert<T>(this DbContext dbContext, Stream stream, params CsvColumnMapping<T>[] additionalMapping) where T : class
        {
            IList<T> entities = GetEntitiesFromStream(stream, additionalMapping);

            EFBatchOperation.For(dbContext, dbContext.Set<T>()).InsertAll(entities);

            return entities;
        }

        /// <summary>
        /// Seeds a dbContext from a CSV file that will be read from the specified embedded resource
        /// </summary>
        /// <typeparam name="T">The type of entity to load</typeparam>
        /// <param name="dbContext">The dbContext to populate</param>
        /// <param name="embeddedResourceName">The name of the embedded resource containing the CSV file contents</param>
        /// <param name="identifierExpression">An expression specifying the properties that should be used when determining whether an Add or Update operation should be performed.</param>
        /// <param name="additionalMapping">Any additonal complex mappings required</param>
        /// <returns>Returns the read entities from the CSV resource. <remarks>It does not return the inserted entities</remarks></returns>
        public static IList<T> SeedFromResource<T>(this DbContext dbContext, string embeddedResourceName, Expression<Func<T, object>> identifierExpression, params CsvColumnMapping<T>[] additionalMapping) where T : class
        {
            IList<T> result;
            using (Stream stream = GetResourceStream(embeddedResourceName))
            {
                result = SeedFromStream(dbContext, stream, identifierExpression, additionalMapping);
            }

            return result;
        }

        /// <summary>
        /// Seeds a dbContext from a CSV file that will be read from the specified embedded resource using BulkInsert operation
        /// </summary>
        /// <typeparam name="T">The type of entity to load</typeparam>
        /// <param name="dbContext">The dbContext to populate</param>
        /// <param name="embeddedResourceName">The name of the embedded resource containing the CSV file contents</param>
        /// <param name="additionalMapping">Any additonal complex mappings required</param>
        /// <returns>Returns the read entities from the CSV resource. <remarks>It does not return the inserted entities</remarks></returns>
        public static IList<T> SeedFromResourceWithBulkInsert<T>(this DbContext dbContext, string embeddedResourceName, params CsvColumnMapping<T>[] additionalMapping) where T : class
        {
            IList<T> result;

            using (Stream stream = GetResourceStream(embeddedResourceName))
            {
                result = SeedFromStreamWithBulkInsert(dbContext, stream, additionalMapping);
            }

            return result;
        }

        private static Stream GetResourceStream(string embeddedResourceName)
        {
            var dataAssemblies = AppDomain.CurrentDomain.GetAssemblies();

            foreach (var assembly in dataAssemblies)
            {
                try
                {
                    Stream result = assembly.GetManifestResourceStream(embeddedResourceName);

                    if (result != null) return result;
                }
                catch (NotSupportedException ex)
                {
                    Console.WriteLine(ex);
                }
            }

            return null;
        }

        /// <summary>
        /// Seeds a dbContext from a CSV file that will be read from the specified file name
        /// </summary>
        /// <typeparam name="T">The type of entity to load</typeparam>
        /// <param name="dbContext">The dbContext to populate</param>
        /// <param name="fileName">The name of the file containing the CSV file contents</param>
        /// <param name="identifierExpression">An expression specifying the properties that should be used when determining whether an Add or Update operation should be performed.</param>
        /// <param name="additionalMapping">Any additonal complex mappings required</param>
        /// <returns>Returns the read entities from the CSV resource. <remarks>It does not return the inserted entities</remarks></returns>
        public static IList<T> SeedFromFile<T>(this DbContext dbContext, string fileName, Expression<Func<T, object>> identifierExpression, params CsvColumnMapping<T>[] additionalMapping) where T : class
        {
            IList<T> result;
            using (Stream stream = File.OpenRead(fileName))
            {
                result = SeedFromStream(dbContext, stream, identifierExpression, additionalMapping);
            }

            return result;
        }

        /// <summary>
        /// Seeds a dbContext from a CSV file that will be read from the specified file name using BulkInsert operation
        /// </summary>
        /// <typeparam name="T">The type of entity to load</typeparam>
        /// <param name="dbContext">The dbContext to populate</param>
        /// <param name="fileName">The name of the file containing the CSV file contents</param>
        /// <param name="additionalMapping">Any additonal complex mappings required</param>
        /// <returns>Returns the read entities from the CSV resource. <remarks>It does not return the inserted entities</remarks></returns>
        public static IList<T> SeedFromFileWithBulkInsert<T>(this DbContext dbContext, string fileName, params CsvColumnMapping<T>[] additionalMapping) where T : class
        {
            IList<T> result;
            using (Stream stream = File.OpenRead(fileName))
            {
                result = SeedFromStreamWithBulkInsert(dbContext, stream, additionalMapping);
            }

            return result;
        }
    }
}