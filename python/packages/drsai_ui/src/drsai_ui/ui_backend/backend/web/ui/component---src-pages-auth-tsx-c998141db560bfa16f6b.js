"use strict";
(self["webpackChunkDr_Sai"] = self["webpackChunkDr_Sai"] || []).push([[577],{

/***/ 74749:
/***/ (function(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

__webpack_require__.r(__webpack_exports__);
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(96540);
/* harmony import */ var gatsby__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(64810);
/* harmony import */ var _hooks_provider__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(92744);
// 接收sso登录的回调，负责保存token和username到本地存储，并跳转到主页




const AuthPage = () => {
  const {
    setUser
  } = react__WEBPACK_IMPORTED_MODULE_0__.useContext(_hooks_provider__WEBPACK_IMPORTED_MODULE_2__/* .appContext */ .v);
  react__WEBPACK_IMPORTED_MODULE_0__.useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const token = params.get("token");
    const username = params.get("username");
    if (token && username) {
      localStorage.setItem("token", token);
      localStorage.setItem("username", username);
      localStorage.setItem("user_email", username); // 假设username就是email
      localStorage.setItem("user_name", username);

      // 清除之前的agent选择，确保新用户登录后使用默认agent
      localStorage.removeItem("drsai-mode-config");

      // 更新用户状态
      setUser({
        name: username,
        email: username,
        username: username
      });
      // 跳转到主页
      (0,gatsby__WEBPACK_IMPORTED_MODULE_1__.navigate)("/");
    } else {
      // 没有参数，跳转到登录
      (0,gatsby__WEBPACK_IMPORTED_MODULE_1__.navigate)("/sso-login");
    }
  }, []);
  return /*#__PURE__*/react__WEBPACK_IMPORTED_MODULE_0__.createElement("div", null, "\u6B63\u5728\u767B\u5F55\uFF0C\u8BF7\u7A0D\u5019...");
};
/* harmony default export */ __webpack_exports__["default"] = (AuthPage);

/***/ })

}]);
//# sourceMappingURL=component---src-pages-auth-tsx-c998141db560bfa16f6b.js.map