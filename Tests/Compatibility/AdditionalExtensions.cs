#pragma warning disable MA0047 // Declare types in namespaces
#pragma warning disable MA0048 // File name must match type name

using System.Runtime.CompilerServices;
using System.Diagnostics;



#if NETFRAMEWORK || NETSTANDARD2_0

using LinqToDB.Mapping;
using System;

public static class AdditionalExtensions
{
	extension(ArgumentNullException exception)
	{
		public static void ThrowIfNull(object? argument, [CallerArgumentExpression(nameof(argument))] string? paramName = null)
		{
			if (argument is null)
			{
				throw new ArgumentNullException(paramName);
			}
		}
	}

	extension(Enum @enum)
	{
		public static TEnum Parse<TEnum>(string value, bool ignoreCase) where TEnum : struct
		{
			ArgumentNullException.ThrowIfNull(value);

			bool success = Enum.TryParse<TEnum>(value.AsSpan(), ignoreCase, out TEnum result);
			
			if (!success)
			{
				throw new ArgumentException($"The value '{value}' is not valid for enum type '{typeof(TEnum).FullName}'.", nameof(value));
			}

			return result;
		}

		public static TEnum Parse<TEnum>(string value) where TEnum : struct
		{
			return Parse<TEnum>(value, true);
		}
	}

	extension(ObjectDisposedException exception)
	{
		public static void ThrowIf(bool condition, Type objectType)
		{
			if (condition)
			{
				throw new ObjectDisposedException(objectType.FullName);
			}
		}
	}

	extension(OperatingSystem operatingSystem)
	{
		public static bool IsWindows() =>
#if TARGET_WINDOWS
            true;
#else
			false;
#endif
		
		public static bool IsLinux() =>         
#if TARGET_LINUX && !TARGET_ANDROID
            true;
#else
			false;
#endif
	}
}

#endif

#pragma warning restore MA0047 // Declare types in namespaces
#pragma warning restore MA0048 // File name must match type name
