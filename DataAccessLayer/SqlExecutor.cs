using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace DataAccessLayer
{
    public static class SqlExecutor
    {
        private enum ExpectedRows { ALL, ONE, NONE }

        /// <summary>
        /// Get a single object from a stored procedure.
        /// </summary>
        /// <typeparam name="T">The type to map the SQL result.</typeparam>
        /// <param name="connString">Which database connection should be used.</param>
        /// <param name="parameters">Sql parameters. Property names will be used as parameter names.</param>
        /// <param name="memberName">Leave blank. The name of the method calling this one must match the stored procedure name.</param>
        /// <returns></returns>
        public static T getObject<T>(this string connectionString, object parameters = null, [CallerMemberName] string memberName = "")
        {
            HashSet<Dictionary<string, string>> items = doSqlStuff(connectionString, memberName, parameters, ExpectedRows.ONE);

            return convertSetToList<T>(items).FirstOrDefault();
        }

        /// <summary>
        /// Get a list of objects from a stored procedure.
        /// </summary>
        /// <typeparam name="T">The type to map the SQL result.</typeparam>
        /// <param name="connString">Which database connection should be used.</param>
        /// <param name="parameters">Sql parameters. Property names will be used as parameter names.</param>
        /// <param name="memberName">Leave blank. The name of the method calling this one must match the stored procedure name.</param>
        /// <returns></returns>
        public static List<T> getList<T>(this string connectionString, object parameters = null, [CallerMemberName] string memberName = "")
        {
            HashSet<Dictionary<string, string>> items = doSqlStuff(connectionString, memberName, parameters, ExpectedRows.ALL);

            return convertSetToList<T>(items);
        }

        /// <summary>
        /// Execute a stored procedure and expect no return.
        /// </summary>
        /// <typeparam name="T">The type to map the SQL result.</typeparam>
        /// <param name="connString">Which database connection should be used.</param>
        /// <param name="parameters">Sql parameters. Property names will be used as parameter names.</param>
        /// <param name="memberName">Leave blank. The name of the method calling this one must match the stored procedure name.</param>
        public static void execute(this string connectionString, object parameters = null, [CallerMemberName] string memberName = "")
        {
            doSqlStuff(connectionString, memberName, parameters, ExpectedRows.NONE);
        }

        /// <summary>
        /// Create connection, send command to SQL server, and parse response.
        /// </summary>
        /// <param name="connString">Which database connection should be used.</param>
        /// <param name="storedProcedure">Which stored procedure to call.</param>
        /// <param name="parameters">Parameters to send to stored procedure.</param>
        /// <param name="expectedRows">How many rows should be parsed from response.</param>
        /// <returns></returns>
        private static HashSet<Dictionary<string, string>> doSqlStuff(string connString, string storedProcedure, object parameters, ExpectedRows expectedRows)
        {
            using (SqlConnection connection = new SqlConnection(connString))
            {
                connection.Open();
                using (SqlCommand command = new SqlCommand(storedProcedure, connection) { CommandType = CommandType.StoredProcedure })
                {
                    SqlParameter[] sqlParameters = parameters?.GetType().GetProperties().ToList()
                        .Select(property => new SqlParameter(property.Name, property.GetValue(parameters)))
                        .ToArray();
                    command.Parameters.AddRange(sqlParameters);

                    switch (expectedRows)
                    {
                        case ExpectedRows.NONE:
                            command.ExecuteNonQuery();
                            return null;
                        case ExpectedRows.ONE:
                            using (SqlDataReader reader = command.ExecuteReader(CommandBehavior.SingleRow))
                            {
                                return convertReaderToSet(reader);
                            }
                        case ExpectedRows.ALL:
                        default:
                            using (SqlDataReader reader = command.ExecuteReader())
                            {
                                return convertReaderToSet(reader);
                            }
                    }
                }
            }
        }

        /// <summary>
        /// Convert SqlDataReader to hash set of dictionaries.
        /// </summary>
        /// <param name="reader">SqlDataReader to parse into dictionary set.</param>
        /// <returns></returns>
        private static HashSet<Dictionary<string, string>> convertReaderToSet(SqlDataReader reader)
        {
            HashSet<Dictionary<string, string>> items = new HashSet<Dictionary<string, string>>();

            while (reader.Read())
            {
                Dictionary<string, string> item = Enumerable.Range(0, reader.FieldCount)
                    .ToDictionary(i => reader.GetName(i).ToLower(), i => reader.GetValue(i).ToString());

                items.Add(item);
            }

            return items;
        }

        /// <summary>
        /// Convert hash set of dictionaries into list of generic objects.
        /// </summary>
        /// <typeparam name="T">Type of list you expect back.</typeparam>
        /// <param name="rows">Dictionary set to convert to a list.</param>
        /// <returns></returns>
        private static List<T> convertSetToList<T>(HashSet<Dictionary<string, string>> rows)
        {
            List<string> propertyNames = typeof(T).GetProperties().Select(property => property.Name.ToLower()).ToList();
            List<string> columnNames = rows.First().Keys.Select(key => key.ToLower()).ToList();
            List<string> bothFieldNames = propertyNames.Intersect(columnNames).ToList();

            return rows.Select(row => convertDictionaryToObject<T>(row, bothFieldNames)).ToList();
        }

        /// <summary>
        /// Convert dictionary into generic object.
        /// </summary>
        /// <typeparam name="T">Type of object you expect back.</typeparam>
        /// <param name="row">Dictionary to convert to an object.</param>
        /// <param name="fieldNames">Names of fields and sql columns to map.</param>
        /// <returns></returns>
        private static T convertDictionaryToObject<T>(Dictionary<string, string> row, List<string> fieldNames)
        {
            T item = (T)Activator.CreateInstance(typeof(T));

            foreach (string fieldName in fieldNames)
            {
                try
                {
                    PropertyInfo property = typeof(T).GetProperties().FirstOrDefault(e => e.Name.ToLower() == fieldName);
                    Type type = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;

                    object value = (row[fieldName] == null) ? null : Convert.ChangeType(row[fieldName], type);

                    property.SetValue(item, value);
                } catch (Exception) 
                {
                    Console.WriteLine($"Error: parsing {fieldName}");
                }
            }

            return item;
        }
    }
}
