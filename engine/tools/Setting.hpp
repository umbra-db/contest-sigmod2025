#pragma once
//---------------------------------------------------------------------------
#include <atomic>
#include <cstdint>
#include <string>
#include <string_view>
#include <limits>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
namespace setting {
struct Bool {
    bool defaultValue;

    constexpr Bool(bool defaultValue = false) noexcept : defaultValue(defaultValue) {}

    bool operator()(std::string_view name, std::string_view value) const;

    constexpr auto getDefault() const noexcept {
        return defaultValue;
    }
};
struct Size {
    size_t defaultValue;
    size_t minValue;
    size_t maxValue;

    constexpr Size(size_t defaultValue = 0, size_t minValue = std::numeric_limits<size_t>::min(), size_t maxValue = std::numeric_limits<size_t>::max()) noexcept : defaultValue(defaultValue), minValue(minValue), maxValue(maxValue) {}

    size_t operator()(std::string_view name, std::string_view value) const;

    constexpr auto getDefault() const noexcept {
        return defaultValue;
    }
};
}
//---------------------------------------------------------------------------
#ifdef SIGMOD_LOCAL
/// Class for runtime settings using environment variables
class SettingBase {
    /// The name of the setting
    std::string name;
    /// The cached value of the setting
    mutable std::string cached;
    /// Whether initialized
    mutable std::atomic<uint8_t> initialized{0};

    /// Ensure the value is ready
    void ensureReadyImpl() const;

    protected:
    /// Compute the value
    virtual void computeImpl(std::string_view str) const = 0;

    /// Ensure the value is ready
    void ensureReady() const {
        if (initialized.load() == 1) [[likely]]
            return;
        return ensureReadyImpl();
    }

    public:
    /// Constructor
    explicit SettingBase(std::string name);

    /// Get as string
    std::string_view getAsString() const;
    /// Get the name
    std::string_view getName() const {
        return name;
    }
    /// Set the value
    void set(std::string value);
};
//---------------------------------------------------------------------------
/// Class for runtime settings using environment variables
template <typename Parser>
class Setting : public SettingBase, Parser {
    /// The cached value
    mutable decltype(std::declval<Parser>()(std::declval<std::string_view>(), std::declval<std::string_view>())) cached;

    /// Compute the value
    void computeImpl(std::string_view str) const override {
        cached = Parser::operator()(getName(), str);
    }

    public:
    /// Constructor
    Setting(std::string name, Parser parser) : SettingBase(std::move(name)), Parser(parser) {
        ensureReady();
    }

    /// Get the value
    auto get() const {
        ensureReady();
        return cached;
    }
};
#else
/// Class for runtime settings using environment variables
template <typename Parser>
class Setting : public Parser {
    public:
    /// Constructor
    constexpr Setting(std::string_view, Parser parser) noexcept : Parser(parser) {}

    /// Get the value
    constexpr auto get() const noexcept {
        return Parser::getDefault();
    }
};
#endif
//---------------------------------------------------------------------------
}
