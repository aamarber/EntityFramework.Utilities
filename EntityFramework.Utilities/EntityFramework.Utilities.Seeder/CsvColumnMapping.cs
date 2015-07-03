using System;

namespace EntityFramework.Utilities.Seeder
{
    /// <summary>
    /// Defines a custom mapping action for a particular column in a CSV file
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class CsvColumnMapping<T>
    {
        private readonly string csvColumnName;
        private readonly Action<T, object> action;

        public delegate void ActionRef<T>(ref T item, object o);

        private readonly ActionRef<T> actionRef;

        /// <summary>
        /// Create new custom mapping action for the specified CSV column name
        /// </summary>
        /// <param name="csvColumnName">The name of the column in the CSV file</param>
        /// <param name="action">The action to execute for each row in the CSV file</param>
        public CsvColumnMapping(string csvColumnName, Action<T, object> action)
        {
            this.csvColumnName = csvColumnName;
            this.action = action;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CsvColumnMapping{T}"/> class.
        /// </summary>
        /// <param name="csvColumnName">Name of the CSV column.</param>
        /// <param name="action">The action.</param>
        public CsvColumnMapping(string csvColumnName, ActionRef<T> action)
        {
            this.csvColumnName = csvColumnName;
            actionRef = action;
        }

        public void Execute(ref T entity, object csvColumnValue)
        {
            if (action != null)
            {
                action(entity, csvColumnValue);
            }
            else if (actionRef != null)
            {
                actionRef(ref entity, csvColumnValue);
            }
        }

        /// <summary>
        /// The name of the csv column
        /// </summary>
        public string CsvColumnName
        {
            get { return csvColumnName; }
        }
    }
}