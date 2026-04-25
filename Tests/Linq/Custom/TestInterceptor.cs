using System.Linq;
using System.Linq.Expressions;
using System.Collections.Generic;
using System.Reflection;
using NUnit.Framework.Interfaces;
using Tests.Data;

namespace Tests.Custom
{
	public class TestInterceptor : ITestInterceptor
	{
		public IEnumerable<string> InterceptDataSources(DataSourcesBaseAttribute dataSourcesAttribute, IEnumerable<string> contexts)
		{
			return contexts;
		}

		public IEnumerable<string> InterceptTestDataSources(DataSourcesBaseAttribute dataSourcesAttribute, IMethodInfo testMethod, IEnumerable<string> contexts)
		{
			contexts = testMethod switch
			{
				//Tests excluded for all providers
				_ when Matches(testMethod, [
					//Tests spicific to linq2db providers
					(Data.TransactionTests _) => _.AttachToExistingTransaction,
					//The tests tries to run out of stack but the iterations are not enough for the all CPUs. Not a provider specific test can be ommitted.
					//(Exceptions.StackUseTests _) => _.TestSqlVisitorHops, //compiler removed
					//Tests spicific to linq2db providers
					(Infrastructure.DataOptionsTests _) => _.TestProviderAutoDetect,
					//Test specific to the DB2 provider
					(SchemaProvider.SchemaProviderTests _) => _.DB2Test,
					//Tests have internal logic based on BulkCopyType - Copied to custom tests
					(xUpdate.BulkCopyTests _) => _.KeepIdentity_SkipOnInsertTrue,
					(xUpdate.BulkCopyTests _) => _.KeepIdentity_SkipOnInsertFalse,
					//Hardcoded DB2 provider dependency
					(xUpdate.BulkCopyTests _) => _.BulkCopyAsyncEnumerableWithCloseAfterUseDataConnection,
					(xUpdate.BulkCopyTests _) => _.BulkCopyAsyncWithCloseAfterUseDataConnection,
					//Tests have property have space - Copied to custom tests
					//(xUpdate.DynamicColumnsTests => _.SqlPropertyNoStoreNonIdentifier, //deleted
					//(xUpdate.DynamicColumnsTests => _.SqlPropertyNoStoreNonIdentifierGrouping, //deleted
					//Valid for DB2 but not DB2i - Test copied to custom
					(xUpdate.DropTableTests _) => _.DropSpecificDatabaseTableTest,
					//Test copied to custom to reduce default source row number
					(xUpdate.MergeTests _) => _.BigSource,
					//Tests passing provider specific parameter types - Generic linq2db test - Not applicable
					(xUpdate.MergeTests _) => _.TestParametersInListSourceProperty,
					//Tests active issue with DB2 family ordering NULL last by default - Not applicable
					(xUpdate.MergeTests _) => _.SortedMergeResultsIssue,
					//Merge in Cte not supported 
					(xUpdate.MergeTests _) => _.MergeIntoCteIssue4107,
					//Tests are not applicable for Db2i (MERGE from/to CTE)
					(xUpdate.MergeTests _) => _.MergeFromCte,
					(xUpdate.MergeTests _) => _.MergeIntoCte,
					(xUpdate.MergeTests _) => _.MergeUsingCteJoin,
					(xUpdate.MergeTests _) => _.MergeUsingCteWhere,
					//Too many cases in code - Copied to custom tests
					(Linq.CharTypesTests _) => _,
					//Too many changes and cases - Copied to custom tests
					(xUpdate.MergeTests _) => _.TestTypesInsertByMerge,
					(xUpdate.MergeTests _) => _.TestMergeTypes,
					(xUpdate.MergeTests _) => _.TestDB2NullsInSource,
					//(UserTests.Issue681Tests _) => _.TestTableFQN, //invalid
					//Query incorrect for DB2i - Copied to custom tests
					//(Data.DataConnectionTests _ => _.EnumExecuteScalarTest,) //compiler removed
					//Case valid for DB2 but not for DB2i
					(UserTests.Issue792Tests _) => _.TestWithTransactionThrowsFromProvider,
					(UserTests.Issue3148Tests _) => _.TestDefaultExpression_09,
					//Data not valid for DB2i
					(UserTests.Issue1287Tests _) => _,
					//Invalid Table Options
					(Linq.TableOptionsTests _) => _.CheckExistenceTest,
					(Linq.TableOptionsTests _) => _.CreateIfNotExistsTest,
					(Linq.TableOptionsTests _) => _.CreateTempIfNotExistsTest,
					(Linq.TableOptionsTests _) => _.DB2TableOptionsTest,
					(Linq.TableOptionsTests _) => _.FluentMappingTest,
					(Linq.TableOptionsTests _) => _.IsGlobalTemporaryTest,
					//Implicit transactions do not function properly in .NET
					//(Data.DataConnectionTests _ => _.TestDisposeFlagCloning962Test1, // compiler removed
					//(Data.DataConnectionTests _ => _.TestDisposeFlagCloning962Test2, // compiler removed
					(Common.ConnectionBuilderTests _) => _.CanUseLoggingFactoryFromIoc,
					(Common.ConnectionBuilderTests _) => _.CanUseWithLoggingFromFactory,
					//Query contains invalid keyword permission
					(UserTests.Issue825Tests _) => _.Test,
					//Test for unsupported WCF feature
					(Linq.AsyncTests _) => _.Test,
					(Linq.AsyncTests _) => _.Test1,
					(Linq.AsyncTests _) => _.TestForEach,
					//Invalid query in test
					//(Data.DataExtensionsTests _) => _.TestDataParameterMapping1, // compiler removed
					//(Data.DataExtensionsTests _) => _.TestObject3, // compiler removed
					//(Data.DataExtensionsTests _) => _.TestObject4, // compiler removed
					//(Data.DataExtensionsTests _) => _.TestObject5, // compiler removed
					//(Data.DataExtensionsTests _) => _.TestObject6, // compiler removed
					//There is no collation in DB2i
					(Linq.SqlExtensionsTests _) => _.TestSqlCollate1,
					(Linq.SqlExtensionsTests _) => _.TestSqlCollate2,
					//Unsupported table options
					(xUpdate.CreateTempTableTests _) => _.CreateTable_NoDisposeError,
					(xUpdate.CreateTempTableTests _) => _.CreateTableAsyncCanceled,
					(xUpdate.CreateTempTableTests _) => _.CreateTableAsyncCanceled2,
					(xUpdate.CreateTempTableTests _) => _.CreateTable_NoDisposeErrorAsync,
					(xUpdate.CreateTempTableTests _) => _.CreateTempTableWithPrimaryKey,
					(xUpdate.CreateTempTableTests _) => _.InsertIntoTempTableWithPrimaryKey,
					(xUpdate.CreateTempTableTests _) => _.CreateTableEnumerableWithNameAndDescriptionAsyncTest,
					// GUIDs are serialized in lower case
					(Linq.ConvertTests _) => _.GuidToString,
					//UpdateRow / UpdateRowLiteral not supported
					(Linq.SqlRowTests _) => _.UpdateRowLiteral,
					(Linq.SqlRowTests _) => _.UpdateRowSelect,
					// Test case uses alias name assertion from generated sql, name sanitization breaks test assertion - Copied to custom tests
					(Extensions.TableIDTests _) => _.TableTest,
					// Test case uses guid conversion with custom conversion expression
					(Linq.ConvertExpressionTests _) => _.Issue3791Test,
					//Ignore custom extension tests 
					(Linq.SqlExtensionTests _) => _,
					//DB2i only supports ASCII identifiers	
					(Linq.IdentifierTests _) => _,
					//Test cases invalid for DB2i
					(Linq.InSubqueryTests _) => _,
					(Linq.SubQueryTests _) => _.DistinctSubqueryTest,
					//Test cases rely on TestUtils.GetSchemaName
					(UserTests.Issue681Tests _) => _,
					//Invalid for DB2i - APPLY not supported
					(Linq.AnalyticTests _) => _.EmptySequenceTest,
					(Linq.EagerLoadingTests _) => _.TestAggregate,
					(Linq.EagerLoadingTests _) => _.TestAggregateAverage,
					(Linq.ElementOperationTests _) => _.NestedFirstOrDefault4,
					(Linq.JoinTests _) => _.Issue3311Test3,
					// Streaming to column not supported
					(Linq.DataTypesTests _) => _.Issue1918Test,
					//Delete From Subquery not supported
					(xUpdate.DeleteTests _) => _.DeleteFromWithTake,
					(xUpdate.DeleteTests _) => _.DeleteFromWithTake_NoSort,
					//Not applicable for DB2i
					(Linq.ParameterTests _) => _.CharAsSqlParameter2,
					(Linq.ParameterTests _) => _.CharAsSqlParameter3,
					//Tests have a hardcoded check for positional parameters, DB2i is mixed so tests cannot work as they are, should be copied over
					(Linq.QueryGenerationTests _) => _.ToSqlQuery_WithParametersDeduplication,
					//Query produces no rows but string to number case is evaluated regardless and breaks
					(Linq.ContainsTests _) => _.Issue2608Test,
					//Full Join adds both sides of predicate null on nullable values. The produced sql is correct but the execution on DB2i seems to be bugged
					(Linq.JoinTests _) => _.SqlFullJoinWithInnerJoinOnRightWithConditions,
					(Linq.JoinTests _) => _.SqlFullJoinWithInnerJoinOnRightWithoutConditions,
					(Linq.JoinTests _) => _.SqlRightJoinWithInnerJoinOnRightWithConditions,
					(Linq.JoinTests _) => _.SqlRightJoinWithInnerJoinOnRightWithoutConditions,
					//Test adds DataParameter converted for enum. Sql builder tries to cast but there is no way to know the parameter provider will be of another type and converted.
					//Could work if parameter value was included in the EvaluationContext
					(UserTests.Issue2372Tests _) => _.Issue2372Test,
					//These tests seem to generate the expected sql, but the test fails as a required field is missing. TODO: open an issue on linq2db for this
					(xUpdate.InsertTests _) => _.Issue3927Test1,
					(xUpdate.InsertTests _) => _.Issue3927Test2,
					//Too many hardcoded provider cases in code - Copied to custom tests
					(Linq.CharTypesTests _) => _,
					//Tests based on predicates used as booleans which is not supported
					(Linq.PredicateTests _) => _,
					//Contains tests that target specific providers. Additionally some are not bypassable.
					//(Data.DataContextTests _) => _, //compiler removed
					//Tests produce query not supported by DB2i (Cross/Outter/Latter Apply joins)
					(Linq.StringJoinTests _) => _.JoinAggregateArray,
					(Linq.StringJoinTests _) => _.JoinAggregateArrayNotNull,
					(Linq.StringJoinTests _) => _.JoinWithGroupingAndUnsupportedMethod,
					(Linq.StringJoinTests _) => _.JoinWithGroupingDistinctSimple,
					(Linq.StringJoinTests _) => _.JoinWithGroupingVarious,
					(UserTests.Issue5256Tests _) => _.NestedSubqueryWithGroupedAggregationsFilteredSumOfSums,
					//Tests produce unexpected sql on DB2i that the test cannot assert
					(UserTests.Issue5152Tests _) => _.TestCase1,
					(UserTests.Issue5152Tests _) => _.TestCase2,
					])
					=> [],


				//** Tests excluded for the AccessClient provider **
				_ when Matches(testMethod, [
					//Access client throws a different exception so it is excluded
					//(Data.DataContextTests _) => _.ProviderConnectionStringConstructorTest2, //compiler removed
					(Data.TraceTests _) => _.TraceInfoErrorsAreReportedForInvalidConnectionString,
					(Data.TraceTests _) => _.TraceInfoErrorsAreReportedForInvalidConnectionStringAsync,
					//Tests run on .net 4.x fail due to inability to compare decimals using hashcode
					(Linq.ConvertTests _) => _.ToDefaultDecimal,
					//Identical query with int literal first and decimal/float literal later fails with AccessClient provider trying to cast to int
					//Possibly caching mechanism by the provider in place, if cases run individually they pass
					(Linq.MappingTests _) => _.MappingTypingByConstant_FromEnumerable_Decimal,
					(Linq.MappingTests _) => _.MappingTypingByConstant_FromEnumerable_Double,
					(Linq.MappingTests _) => _.MappingTypingByConstant_FromEnumerable_Float,
					(Linq.MappingTests _) => _.MappingTypingByConstant_FromEnumerable_Int64,
					(Linq.MappingTests _) => _.MappingTypingByConstant_FromEnumerable_UInt32,
					(Linq.MappingTests _) => _.MappingTypingByConstant_FromEnumerable_UInt64,
					(Linq.MappingTests _) => _.MappingTypingByConstant_FromQuery_Decimal,
					(Linq.MappingTests _) => _.MappingTypingByConstant_FromQuery_Double,
					(Linq.MappingTests _) => _.MappingTypingByConstant_FromQuery_Float,
					(Linq.MappingTests _) => _.MappingTypingByConstant_FromQuery_Int64,
					(Linq.MappingTests _) => _.MappingTypingByConstant_FromQuery_UInt32,
					(Linq.MappingTests _) => _.MappingTypingByConstant_FromQuery_UInt64,
				])
				=> contexts.Except(TestProvNameDb2i.GetProviders(TestProvNameDb2i.All_AccessClient)),


				//** Tests excluded for the OleDb provider **
				_ when Matches(testMethod, [
					//OleDb doesn't support inline comments so tags are not supported (fails with PREPARE STATEMENT error)
					(Linq.TagTests _) => _,
					//OleDb doesn't support inline comments so test that perform comment assertions are not supported
					(Extensions.QueryNameTests _) => _.TableTest,
					(Extensions.QueryNameTests _) => _.FromTest,
					(Extensions.QueryNameTests _) => _.MainInlineTest,
					// OleDb test timesout due to large operation 
					(Linq.BooleanTests _) => _.Test,
					// Test compares produced sql with hardcoded test case. OleDb has additional spaces that break it.
					(Linq.WhereTests _) => _.Issue2897_ParensGeneration_MixedFromAnd,
					(Linq.WhereTests _) => _.Issue2897_ParensGeneration_MixedFromOr,
					// Test contains a hardcoded extension with a function that does not have a space after a comma.
					(UserTests.Issue4336Tests _) => _.Issue4336Test,
					//OleDb treats null char as space
					(Linq.ParameterTests _) => _.CharAsSqlParameter5
				])
				=> contexts.Except(TestProvNameDb2i.GetProviders(TestProvNameDb2i.All_OleDb)),


				//** Tests excluded for version lower than 7.4 **
				_ when Matches(testMethod, [
					//LAG returns numeric instead of timestamp prior to 7.4
					(Linq.AnalyticTests _) => _.Issue1799Test1,
					(Linq.AnalyticTests _) => _.Issue1799Test2,
				])
				=> contexts.Intersect(TestProvNameDb2i.GetProviders(TestProvNameDb2i.All_74)),


				//** Tests excluded for GAS provider versions **
				_ when Matches(testMethod, [
					//LAG returns numeric instead of timestamp prior to 7.4
					(Linq.ConcurrencyTests _) => _.TestGuid,
					(Linq.ConcurrencyTests _) => _.TestTestGuidAsync,
				])
				=> contexts.Where(c => !c.Contains("GAS")),


				//** Default **
				_ => contexts
			};

			return contexts;
		}

		private static bool MatchesMethod(LambdaExpression expression, MethodInfo methodInfo)
		{
			//Expression with a single parameter expected
			if (expression.Parameters.Count != 1)
				return false;

			// Whole class is selected 
			if (expression.Body is ParameterExpression parameterExpression
				&& expression.Parameters[0].Type == methodInfo.ReflectedType)
				return parameterExpression == expression.Parameters[0];
			// Actual method invocation
			else if (expression.Body is MethodCallExpression methodCall)
				return methodCall.Method == methodInfo;
			// MethodGroup selection (e.g. method without arguments)
			else if (expression.Body is UnaryExpression unaryExpression
				&& unaryExpression.NodeType == ExpressionType.Convert
				&& unaryExpression.Operand is MethodCallExpression methodCall1
				&& methodCall1.Object is ConstantExpression constantExpression
				&& constantExpression.Value is MethodInfo methodInfo1)
				return methodInfo1 == methodInfo;

			return false;
		}

		private static bool Matches(IMethodInfo methodInfo, params LambdaExpression[] expressions)
		{
			return expressions.Any(e => MatchesMethod(e, methodInfo.MethodInfo));
		}
	}
}
