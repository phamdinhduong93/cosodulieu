using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;

namespace DAL
{
    public class SqlHelper
    {

        private SqlConnection _objSqlConn;
        private string strConn;
        public SqlHelper()
        {
            strConn = @"Server=.\SQLExpress;Database=QLVBDevyt;Persist Security Info=True;User ID=sa;Password=123456";
        }
        protected virtual void Dispose()
        {
            try
            {
                _objSqlConn.Dispose();
                if ((_objSqlConnWithTrans != null))
                {
                    _objSqlConnWithTrans.Dispose();
                }
            }
            catch (Exception)
            {
            }
        }


        #region "# Transaction #"
        private SqlTransaction _objTransaction = null;
        private SqlConnection _objSqlConnWithTrans = null;
        public bool BeginTransaction()
        {
            _objSqlConnWithTrans = new SqlConnection(this.strConn);
            try
            {
                _objSqlConnWithTrans.Open();
                _objTransaction = _objSqlConnWithTrans.BeginTransaction();
                return true;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        public bool ComitTransaction()
        {
            try
            {
                if (_objSqlConnWithTrans == null)
                    return false;
                _objTransaction.Commit();
                if (_objSqlConnWithTrans.State != ConnectionState.Closed)
                {
                    _objSqlConnWithTrans.Close();
                    _objTransaction = null;
                    _objSqlConnWithTrans = null;
                }
                return true;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        public bool RollBackTransaction()
        {
            try
            {
                if (_objSqlConnWithTrans == null)
                    return false;
                _objTransaction.Rollback();
                if (_objSqlConnWithTrans.State != ConnectionState.Closed)
                {
                    _objSqlConnWithTrans.Close();
                    _objTransaction = null;
                    _objSqlConnWithTrans = null;
                }
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }
        #endregion

        #region "# ExecuteSQLDataTable #"
        public DataTable ExecuteSQLDataTable(string sSQL, params object[] parameters)
        {
            DataTable dt = new DataTable();
            SqlCommand _sqlCmd = new SqlCommand();

            try
            {
                _objSqlConn = new SqlConnection(this.strConn);
                _sqlCmd.CommandText = sSQL;
                _sqlCmd.Connection = _objSqlConn;
                _objSqlConn.Open();
                for (int i = 0; i < parameters.Length - 1; i += 2)
                {
                    SqlParameter p = new SqlParameter();
                    p.ParameterName = parameters[i].ToString();
                    p.Value = parameters[i + 1];
                    _sqlCmd.Parameters.Add(p);
                }
                SqlDataAdapter da = new SqlDataAdapter(_sqlCmd);
                da.Fill(dt);
                return dt;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                _sqlCmd.Dispose();
                if (_objSqlConn.State != ConnectionState.Closed)
                {
                    _objSqlConn.Close();
                    _objSqlConn.Dispose();
                }
            }
        }
        #endregion

        #region "# ExecuteSQLDataSet #"
        public DataSet ExecuteSQLDataSet(string sSQL, params object[] parameters)
        {
            DataSet ds = new DataSet();
            SqlCommand _sqlCmd = new SqlCommand();
            try
            {
                _objSqlConn = new SqlConnection(this.strConn);
                _sqlCmd.CommandText = sSQL;
                _sqlCmd.Connection = _objSqlConn;
                _objSqlConn.Open();
                for (int i = 0; i < parameters.Length - 1; i += 2)
                {
                    SqlParameter p = new SqlParameter();
                    p.ParameterName = parameters[i].ToString();
                    p.Value = parameters[i + 1];
                    _sqlCmd.Parameters.Add(p);
                }
                SqlDataAdapter da = new SqlDataAdapter(_sqlCmd);
                da.Fill(ds);
                return ds;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                _sqlCmd.Dispose();
                if (_objSqlConn.State != ConnectionState.Closed)
                {
                    _objSqlConn.Close();
                    _objSqlConn.Dispose();
                }
            }
        }
        #endregion

        #region "# ExecuteSQLNonQuery #"
        public object ExecuteSQLNonQuery(string sSQL, params object[] parameters)
        {
            SqlCommand _sqlCmd = new SqlCommand();
            try
            {
                if (_objSqlConnWithTrans == null)
                {
                    _objSqlConn = new SqlConnection(this.strConn);
                    _sqlCmd.Connection = _objSqlConn;
                    _objSqlConn.Open();
                }
                else
                {
                    _sqlCmd.Connection = _objSqlConnWithTrans;
                    _sqlCmd.Transaction = _objTransaction;
                }
                _sqlCmd.CommandText = sSQL;
                for (int i = 0; i < parameters.Length - 1; i += 2)
                {
                    SqlParameter p = new SqlParameter();
                    p.ParameterName = parameters[i].ToString();
                    p.Value = parameters[i + 1];
                    _sqlCmd.Parameters.Add(p);
                }
                return Convert.ToInt32(_sqlCmd.ExecuteNonQuery());
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                _sqlCmd.Dispose();
                if (_objSqlConnWithTrans == null)
                {
                    if (_objSqlConn.State != ConnectionState.Closed)
                    {
                        _objSqlConn.Close();
                        _objSqlConn.Dispose();
                    }
                }
            }
        }
        #endregion

        #region "# ExecuteSQLScalar #"
        public object ExecuteSQLScalar(string sSQL, params object[] parameters)
        {
            SqlCommand _sqlCmd = new SqlCommand();
            try
            {
                if (_objSqlConnWithTrans == null)
                {
                    _objSqlConn = new SqlConnection(this.strConn);
                    _objSqlConn.Open();
                    _sqlCmd.Connection = _objSqlConn;
                }
                else
                {
                    _sqlCmd.Connection = _objSqlConnWithTrans;
                    _sqlCmd.Transaction = _objTransaction;
                }
                _sqlCmd.CommandText = sSQL;
                for (int i = 0; i < parameters.Length - 1; i += 2)
                {
                    SqlParameter p = new SqlParameter();
                    p.ParameterName = parameters[i].ToString();
                    p.Value = parameters[i + 1];
                    _sqlCmd.Parameters.Add(p);
                }
                return _sqlCmd.ExecuteScalar();
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                _sqlCmd.Dispose();
                if (_objSqlConn.State != ConnectionState.Closed)
                {
                    _objSqlConn.Close();
                    _objSqlConn.Dispose();
                }
            }
        }
        #endregion

        #region "# ExecutePrcDataTable #"
        public DataTable ExecutePrcDataTable(string sSQL, params object[] parameters)
        {
            DataTable dt = new DataTable();
            SqlCommand _sqlCmd = new SqlCommand();
            try
            {
                _objSqlConn = new SqlConnection(this.strConn);
                _sqlCmd.CommandText = sSQL;
                _sqlCmd.CommandType = CommandType.StoredProcedure;
                _sqlCmd.Connection = _objSqlConn;
                _objSqlConn.Open();
                for (int i = 0; i < parameters.Length - 1; i += 2)
                {
                    SqlParameter p = new SqlParameter();
                    p.ParameterName = parameters[i].ToString();
                    p.Value = parameters[i + 1];
                    _sqlCmd.Parameters.Add(p);
                }
                SqlDataAdapter da = new SqlDataAdapter(_sqlCmd);
                da.Fill(dt);
                return dt;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                _sqlCmd.Dispose();
                if (_objSqlConn.State != ConnectionState.Closed)
                {
                    _objSqlConn.Close();
                    _objSqlConn.Dispose();
                }
            }
        }
        #endregion

        #region "# ExecutePrcDataSet #"
        public DataSet ExecutePrcDataSet(string sSQL, params object[] parameters)
        {
            DataSet ds = new DataSet();
            SqlCommand _sqlCmd = new SqlCommand();
            try
            {
                _objSqlConn = new SqlConnection(this.strConn);
                _sqlCmd.CommandText = sSQL;
                _sqlCmd.CommandType = CommandType.StoredProcedure;
                _sqlCmd.Connection = _objSqlConn;
                _objSqlConn.Open();
                for (int i = 0; i < parameters.Length - 1; i += 2)
                {
                    SqlParameter p = new SqlParameter();
                    p.ParameterName = parameters[i].ToString();
                    p.Value = parameters[i + 1];
                    _sqlCmd.Parameters.Add(p);
                }
                SqlDataAdapter da = new SqlDataAdapter(_sqlCmd);
                da.Fill(ds);
                return ds;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                _sqlCmd.Dispose();
                if (_objSqlConn.State != ConnectionState.Closed)
                {
                    _objSqlConn.Close();
                    _objSqlConn.Dispose();
                }
            }
        }
        #endregion

        #region "# doInsert #"
        public object doInsert(string TenBang, params object[] parameters)
        {
            try
            {
                string sql = "SET DATEFORMAT DMY ";
                string str1 = "";
                string str2 = "";

                for (int i = 0; i < parameters.Length - 1; i += 2)
                {
                    str1 += parameters[i].ToString().Substring(1) + ", ";
                    str2 += parameters[i].ToString() + ", ";
                }

                if (str1.Length > 2)
                    str1 = str1.Substring(0, str1.Length - 2);
                if (str2.Length > 2)
                    str2 = str2.Substring(0, str2.Length - 2);
                sql += "INSERT INTO [" + TenBang + "](" + str1 + ") VALUES(" + str2 + "); SELECT SCOPE_IDENTITY();";

                return ExecuteSQLScalar(sql, parameters);
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        public object doInsertWithIdentity(string TenBang, params object[] parameters)
        {
            try
            {
                string sql = "SET DATEFORMAT DMY SET IDENTITY_INSERT " + TenBang + " ON ";
                string str1 = "";
                string str2 = "";

                for (int i = 0; i < parameters.Length - 1; i += 2)
                {
                    str1 += parameters[i].ToString().Substring(1) + ", ";
                    str2 += parameters[i].ToString() + ", ";
                }
                if (str1.Length > 2)
                    str1 = str1.Substring(0, str1.Length - 2);
                if (str2.Length > 2)
                    str2 = str2.Substring(0, str2.Length - 2);
                sql += "INSERT INTO [" + TenBang + "](" + str1 + ") VALUES(" + str2 + ")  SET IDENTITY_INSERT " + TenBang + " OFF ; SELECT SCOPE_IDENTITY();";
                return ExecuteSQLScalar(sql, parameters);
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }
        #endregion

        #region "# doUpdate #"
        public object doUpdate(string TenBang, string @where, params object[] parameters)
        {
            try
            {
                string sql = "SET DATEFORMAT DMY UPDATE [" + TenBang + "] ";
                string str1 = "";
                for (int i = 0; i < parameters.Length - 1; i += 2)
                {
                    str1 += parameters[i].ToString().Substring(1) + " = " + parameters[i].ToString() + ", ";
                }
                if (str1.Length > 2)
                    str1 = str1.Substring(0, str1.Length - 2);
                sql += "SET " + str1 + " ";
                sql += "WHERE " + @where + " ";
                return ExecuteSQLNonQuery(sql, parameters);
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }
        #endregion

        #region "# doDelete #"
        public object doDelete(string TenBang, string @where, params object[] parameters)
        {
            try
            {
                string sql = "DELETE FROM [" + TenBang + "] WHERE " + @where;
                return ExecuteSQLNonQuery(sql, parameters);
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }
        #endregion
    }
}
