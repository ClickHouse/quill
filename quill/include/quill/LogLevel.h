#pragma once

#include "quill/detail/utility/Attributes.h"
#include <array>
#include <cstdint>
#include <string>
#include <type_traits>

namespace quill
{
/**
 * Log level enum
 */
enum class LogLevel : uint32_t
{
  TraceL3,
  TraceL2,
  TraceL1,
  Debug,
  Info,
  Warning,
  Error,
  Critical,
  None
};

/**
 * Converts a LogLevel enum to string
 * @param log_level LogLevel
 * @return the corresponding string value
 */
QUILL_NODISCARD char const* to_string(LogLevel log_level);

/**
 * Converts a string to a LogLevel enum value
 * @param log_level the log level string to convert
 * @return the corresponding LogLevel enum value
 */
QUILL_NODISCARD LogLevel from_string(std::string log_level);

} // namespace quill