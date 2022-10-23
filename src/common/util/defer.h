#ifndef YDB_PERF_COMMON_DEFER_H_
#define YDB_PERF_COMMON_DEFER_H_

#include <type_traits>
#include <utility>

namespace ydb_util {
// A helper defer function to simulate the defer keyword in golang
template <typename Func>
class DeferredAction {
 public:
  static_assert(std::is_nothrow_move_constructible_v<Func>,
                "Func(Func&&) must be noexcept");
  explicit DeferredAction(Func&& func) noexcept : func_(std::move(func)) {}
  DeferredAction(DeferredAction&& other) noexcept
      : func_(std::move(other.func_)), canceled_(other.canceled_) {
    other.canceled_ = true;
  }

  ~DeferredAction() {
    if (!canceled_) {
      func_();
    }
  }

  DeferredAction& operator=(DeferredAction&& other) noexcept {
    if (this != &other) {
      this->~DeferredAction();
      new (this) DeferredAction(std::move(other));
    }
    return *this;
  }

  DeferredAction(const DeferredAction& other) = delete;
  void Cancel() noexcept { canceled_ = true; }

 private:
  Func func_;
  bool canceled_ = false;
};

template <typename Func>
inline DeferredAction<Func> defer(Func&& func) noexcept {
  return DeferredAction<Func>(std::forward<Func>(func));
}
}  // namespace ydb_util

#ifndef DEFER
#define ___DEFER(func, line, counter) \
  auto __defer_##line##_##counter = ydb_util::defer(func)
#define DEFER(func) ___DEFER(func, __LINE__, __COUNTER__)
#endif

#endif