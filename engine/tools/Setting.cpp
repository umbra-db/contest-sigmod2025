#include "tools/Setting.hpp"
#include <cassert>
#include <charconv>
#include <stdexcept>
//---------------------------------------------------------------------------
namespace engine {
//---------------------------------------------------------------------------
#ifdef SIGMOD_LOCAL
//---------------------------------------------------------------------------
SettingBase::SettingBase(std::string name) : name(std::move(name)) {}
//---------------------------------------------------------------------------
std::string_view SettingBase::getAsString() const {
    ensureReady();
    return cached;
}
//---------------------------------------------------------------------------
void SettingBase::set(std::string value) {
    cached = std::move(value);
    computeImpl(cached);
}
//---------------------------------------------------------------------------
void SettingBase::ensureReadyImpl() const {
    // We assume set and get cannot run concurrently
    // But gets may run concurrently
    auto cur = initialized.load();
    if (cur == 1)
        return;
    auto waitUnlocked = [&]() {
        while (initialized.load() == 2);
        assert(initialized.load() == 1);
        return;
    };
    if (cur == 2)
        return waitUnlocked();
    assert(cur == 0);
    while (!initialized.compare_exchange_weak(cur, 2)) {
        if (cur == 2)
            return waitUnlocked();
        if (cur == 1)
            return;
    }
    assert(cur == 0);
    auto* res = getenv(name.data());
    if (res) {
        cached = res;
    } else {
        cached = "";
    }
    computeImpl(cached);
    initialized.store(1);
}
//---------------------------------------------------------------------------
#endif
//---------------------------------------------------------------------------
namespace setting {
//---------------------------------------------------------------------------
bool Bool::operator()(std::string_view name, std::string_view value) const {
    if (value.empty())
        return defaultValue;
    if (value[0] == 't' || value[0] == 'T' || value == "1") {
        return true;
    } else if (value[0] == 'f' || value[0] == 'F' || value == "0") {
        return false;
    }
    return defaultValue;
}
//---------------------------------------------------------------------------
size_t Size::operator()(std::string_view name, std::string_view value) const {
    if (value.empty())
        return defaultValue;
    // Read size indicators k, m, g, t from end
    size_t multiplier = 1;
    char valueEndChar = value[value.size() - 1];
    if (valueEndChar == 'k' || valueEndChar == 'K') {
        multiplier = 1024;
        value.remove_suffix(1);
    } else if (valueEndChar == 'm' || valueEndChar == 'M') {
        multiplier = 1024 * 1024;
        value.remove_suffix(1);
    } else if (valueEndChar == 'g' || valueEndChar == 'G') {
        multiplier = 1024 * 1024 * 1024;
        value.remove_suffix(1);
    }
    // Read the number
    size_t num;
    std::from_chars_result result = std::from_chars(value.data(), value.data() + value.size(), num);
    if (result.ec == std::errc()) {
        auto res = num * multiplier;
        if (res < minValue || res > maxValue) {
            return defaultValue;
        }
        return res;
    } else {
        return defaultValue;
    }
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
}