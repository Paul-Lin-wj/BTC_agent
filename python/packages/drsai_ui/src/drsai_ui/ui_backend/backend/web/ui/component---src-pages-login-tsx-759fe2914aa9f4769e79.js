"use strict";
(self["webpackChunkDr_Sai"] = self["webpackChunkDr_Sai"] || []).push([[626],{

/***/ 46981:
/***/ (function(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

// ESM COMPAT FLAG
__webpack_require__.r(__webpack_exports__);

// EXPORTS
__webpack_require__.d(__webpack_exports__, {
  "default": function() { return /* binding */ login; }
});

// EXTERNAL MODULE: ./node_modules/react/index.js
var react = __webpack_require__(96540);
// EXTERNAL MODULE: ./node_modules/antd/es/tabs/index.js + 24 modules
var tabs = __webpack_require__(10277);
// EXTERNAL MODULE: ./node_modules/antd/es/form/index.js + 23 modules
var es_form = __webpack_require__(74054);
// EXTERNAL MODULE: ./node_modules/antd/es/message/index.js + 4 modules
var message = __webpack_require__(69036);
// EXTERNAL MODULE: ./node_modules/antd/es/card/index.js + 4 modules
var card = __webpack_require__(677);
// EXTERNAL MODULE: ./node_modules/antd/es/input/index.js + 22 modules
var input = __webpack_require__(46789);
// EXTERNAL MODULE: ./node_modules/antd/es/button/index.js + 27 modules
var es_button = __webpack_require__(81917);
// EXTERNAL MODULE: ./node_modules/@babel/runtime/helpers/esm/extends.js
var esm_extends = __webpack_require__(58168);
;// ./node_modules/@ant-design/icons-svg/es/asn/UserOutlined.js
// This icon file is generated automatically.
var UserOutlined = { "icon": { "tag": "svg", "attrs": { "viewBox": "64 64 896 896", "focusable": "false" }, "children": [{ "tag": "path", "attrs": { "d": "M858.5 763.6a374 374 0 00-80.6-119.5 375.63 375.63 0 00-119.5-80.6c-.4-.2-.8-.3-1.2-.5C719.5 518 760 444.7 760 362c0-137-111-248-248-248S264 225 264 362c0 82.7 40.5 156 102.8 201.1-.4.2-.8.3-1.2.5-44.8 18.9-85 46-119.5 80.6a375.63 375.63 0 00-80.6 119.5A371.7 371.7 0 00136 901.8a8 8 0 008 8.2h60c4.4 0 7.9-3.5 8-7.8 2-77.2 33-149.5 87.8-204.3 56.7-56.7 132-87.9 212.2-87.9s155.5 31.2 212.2 87.9C779 752.7 810 825 812 902.2c.1 4.4 3.6 7.8 8 7.8h60a8 8 0 008-8.2c-1-47.8-10.9-94.3-29.5-138.2zM512 534c-45.9 0-89.1-17.9-121.6-50.4S340 407.9 340 362c0-45.9 17.9-89.1 50.4-121.6S466.1 190 512 190s89.1 17.9 121.6 50.4S684 316.1 684 362c0 45.9-17.9 89.1-50.4 121.6S557.9 534 512 534z" } }] }, "name": "user", "theme": "outlined" };
/* harmony default export */ var asn_UserOutlined = (UserOutlined);

// EXTERNAL MODULE: ./node_modules/@ant-design/icons/es/components/AntdIcon.js + 3 modules
var AntdIcon = __webpack_require__(87064);
;// ./node_modules/@ant-design/icons/es/icons/UserOutlined.js

// GENERATE BY ./scripts/generate.ts
// DON NOT EDIT IT MANUALLY




var UserOutlined_UserOutlined = function UserOutlined(props, ref) {
  return /*#__PURE__*/react.createElement(AntdIcon/* default */.A, (0,esm_extends/* default */.A)({}, props, {
    ref: ref,
    icon: asn_UserOutlined
  }));
};

/**![user](data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNTAiIGhlaWdodD0iNTAiIGZpbGw9IiNjYWNhY2EiIHZpZXdCb3g9IjY0IDY0IDg5NiA4OTYiIGZvY3VzYWJsZT0iZmFsc2UiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyI+PHBhdGggZD0iTTg1OC41IDc2My42YTM3NCAzNzQgMCAwMC04MC42LTExOS41IDM3NS42MyAzNzUuNjMgMCAwMC0xMTkuNS04MC42Yy0uNC0uMi0uOC0uMy0xLjItLjVDNzE5LjUgNTE4IDc2MCA0NDQuNyA3NjAgMzYyYzAtMTM3LTExMS0yNDgtMjQ4LTI0OFMyNjQgMjI1IDI2NCAzNjJjMCA4Mi43IDQwLjUgMTU2IDEwMi44IDIwMS4xLS40LjItLjguMy0xLjIuNS00NC44IDE4LjktODUgNDYtMTE5LjUgODAuNmEzNzUuNjMgMzc1LjYzIDAgMDAtODAuNiAxMTkuNUEzNzEuNyAzNzEuNyAwIDAwMTM2IDkwMS44YTggOCAwIDAwOCA4LjJoNjBjNC40IDAgNy45LTMuNSA4LTcuOCAyLTc3LjIgMzMtMTQ5LjUgODcuOC0yMDQuMyA1Ni43LTU2LjcgMTMyLTg3LjkgMjEyLjItODcuOXMxNTUuNSAzMS4yIDIxMi4yIDg3LjlDNzc5IDc1Mi43IDgxMCA4MjUgODEyIDkwMi4yYy4xIDQuNCAzLjYgNy44IDggNy44aDYwYTggOCAwIDAwOC04LjJjLTEtNDcuOC0xMC45LTk0LjMtMjkuNS0xMzguMnpNNTEyIDUzNGMtNDUuOSAwLTg5LjEtMTcuOS0xMjEuNi01MC40UzM0MCA0MDcuOSAzNDAgMzYyYzAtNDUuOSAxNy45LTg5LjEgNTAuNC0xMjEuNlM0NjYuMSAxOTAgNTEyIDE5MHM4OS4xIDE3LjkgMTIxLjYgNTAuNFM2ODQgMzE2LjEgNjg0IDM2MmMwIDQ1LjktMTcuOSA4OS4xLTUwLjQgMTIxLjZTNTU3LjkgNTM0IDUxMiA1MzR6IiAvPjwvc3ZnPg==) */
var RefIcon = /*#__PURE__*/react.forwardRef(UserOutlined_UserOutlined);
if (false) {}
/* harmony default export */ var icons_UserOutlined = (RefIcon);
;// ./node_modules/@ant-design/icons-svg/es/asn/LockOutlined.js
// This icon file is generated automatically.
var LockOutlined = { "icon": { "tag": "svg", "attrs": { "viewBox": "64 64 896 896", "focusable": "false" }, "children": [{ "tag": "path", "attrs": { "d": "M832 464h-68V240c0-70.7-57.3-128-128-128H388c-70.7 0-128 57.3-128 128v224h-68c-17.7 0-32 14.3-32 32v384c0 17.7 14.3 32 32 32h640c17.7 0 32-14.3 32-32V496c0-17.7-14.3-32-32-32zM332 240c0-30.9 25.1-56 56-56h248c30.9 0 56 25.1 56 56v224H332V240zm460 600H232V536h560v304zM484 701v53c0 4.4 3.6 8 8 8h40c4.4 0 8-3.6 8-8v-53a48.01 48.01 0 10-56 0z" } }] }, "name": "lock", "theme": "outlined" };
/* harmony default export */ var asn_LockOutlined = (LockOutlined);

;// ./node_modules/@ant-design/icons/es/icons/LockOutlined.js

// GENERATE BY ./scripts/generate.ts
// DON NOT EDIT IT MANUALLY




var LockOutlined_LockOutlined = function LockOutlined(props, ref) {
  return /*#__PURE__*/react.createElement(AntdIcon/* default */.A, (0,esm_extends/* default */.A)({}, props, {
    ref: ref,
    icon: asn_LockOutlined
  }));
};

/**![lock](data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNTAiIGhlaWdodD0iNTAiIGZpbGw9IiNjYWNhY2EiIHZpZXdCb3g9IjY0IDY0IDg5NiA4OTYiIGZvY3VzYWJsZT0iZmFsc2UiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyI+PHBhdGggZD0iTTgzMiA0NjRoLTY4VjI0MGMwLTcwLjctNTcuMy0xMjgtMTI4LTEyOEgzODhjLTcwLjcgMC0xMjggNTcuMy0xMjggMTI4djIyNGgtNjhjLTE3LjcgMC0zMiAxNC4zLTMyIDMydjM4NGMwIDE3LjcgMTQuMyAzMiAzMiAzMmg2NDBjMTcuNyAwIDMyLTE0LjMgMzItMzJWNDk2YzAtMTcuNy0xNC4zLTMyLTMyLTMyek0zMzIgMjQwYzAtMzAuOSAyNS4xLTU2IDU2LTU2aDI0OGMzMC45IDAgNTYgMjUuMSA1NiA1NnYyMjRIMzMyVjI0MHptNDYwIDYwMEgyMzJWNTM2aDU2MHYzMDR6TTQ4NCA3MDF2NTNjMCA0LjQgMy42IDggOCA4aDQwYzQuNCAwIDgtMy42IDgtOHYtNTNhNDguMDEgNDguMDEgMCAxMC01NiAweiIgLz48L3N2Zz4=) */
var LockOutlined_RefIcon = /*#__PURE__*/react.forwardRef(LockOutlined_LockOutlined);
if (false) {}
/* harmony default export */ var icons_LockOutlined = (LockOutlined_RefIcon);
// EXTERNAL MODULE: ./src/hooks/provider.tsx
var provider = __webpack_require__(92744);
// EXTERNAL MODULE: ./src/components/views/api.ts
var api = __webpack_require__(39614);
;// ./src/pages/login.tsx





const {
  TabPane
} = tabs/* default */.A;

// 登录页面专用的 Tabs 样式
const loginTabsStyle = "\n    .login-page-tabs .ant-tabs-tab-btn {\n        color: #fff !important;\n        transition: none !important;\n    }\n    .login-page-tabs .ant-tabs-tab.ant-tabs-tab-active .ant-tabs-tab-btn {\n        color: #fff !important;\n        font-weight: 500;\n    }\n    .login-page-tabs .ant-tabs-tab:hover .ant-tabs-tab-btn {\n        color: rgba(255, 255, 255, 0.8) !important;\n    }\n    .login-page-tabs .ant-tabs-ink-bar {\n        background-color: #fff !important;\n        transition: none !important;\n    }\n    .login-page-tabs .ant-tabs-nav::before {\n        border-color: rgba(255, 255, 255, 0.2) !important;\n    }\n    .login-page-button {\n        padding: 15px 30px !important;\n        font-size: 1.1rem !important;\n        background: rgba(255,255,255,0.1) !important;\n        color: #fff !important;\n        border: none !important;\n        border-radius: 25px !important;\n        cursor: pointer !important;\n        transition: all 0.3s ease !important;\n        backdrop-filter: blur(5px) !important;\n        box-shadow: 0 4px 15px rgba(0,0,0,0.2) !important;\n    }\n    .login-page-button:hover {\n        background: rgba(255,255,255,0.2) !important;\n        transform: translateY(-2px) !important;\n        box-shadow: 0 6px 20px rgba(0,0,0,0.3) !important;\n    }\n";
const backgroundStyle = {
  minHeight: "100vh",
  display: "flex",
  justifyContent: "center",
  alignItems: "center",
  background: "linear-gradient(rgba(0,0,0,0.5), rgba(0,0,0,0.5)),\n    url('https://images.unsplash.com/photo-1497864149936-d3163f0c0f4b?ixlib=rb-1.2.1&auto=format&fit=crop&w=1950&q=80')",
  backgroundSize: "cover",
  backgroundPosition: "center",
  padding: "20px"
};
const logoContainerStyle = {
  position: "fixed",
  top: 20,
  left: 20,
  zIndex: 1000,
  color: "white",
  fontSize: 16,
  lineHeight: 1.2
};
const containerStyle = {
  textAlign: "center",
  padding: "2rem",
  background: "rgba(255,255,255,0.1)",
  borderRadius: 15,
  backdropFilter: "blur(10px)",
  boxShadow: "0 8px 32px 0 rgba(31, 38, 135, 0.37)",
  border: "1px solid rgba(255,255,255,0.2)"
};
const h2Style = {
  color: "#fff",
  fontSize: "2.5rem",
  marginBottom: "1.5rem",
  textShadow: "2px 2px 4px rgba(0,0,0,0.5)"
};
const cardStyle = {
  width: 400,
  background: "rgba(255,255,255,0.1)",
  borderRadius: 15,
  backdropFilter: "blur(10px)",
  boxShadow: "0 8px 32px 0 rgba(31, 38, 135, 0.37)",
  border: "1px solid rgba(255,255,255,0.2)"
};
const LoginPage = () => {
  const {
    setUser
  } = react.useContext(provider/* appContext */.v);
  const {
    0: loginLoading,
    1: setLoginLoading
  } = (0,react.useState)(false);
  const {
    0: registerLoading,
    1: setRegisterLoading
  } = (0,react.useState)(false);
  const {
    0: activeTab,
    1: setActiveTab
  } = (0,react.useState)("login");
  const [loginForm] = es_form/* default */.A.useForm();

  // 路由保护由 RouteGuard 组件统一处理
  // 如果已经登录，RouteGuard 会自动重定向到主页

  const handleLogin = async values => {
    setLoginLoading(true);
    try {
      const response = await api/* authAPI */.R2.login(values.username, values.password);
      if (response.status) {
        // 保存用户信息到本地存储
        localStorage.setItem("token", "local_" + Date.now());
        localStorage.setItem("username", values.username);
        localStorage.setItem("user_email", values.username);
        localStorage.setItem("user_name", values.username);

        // 更新用户状态
        setUser({
          name: values.username,
          email: values.username,
          username: values.username
        });
        message/* default */.Ay.success("登录成功！");
        window.location.href = "/";
      }
    } catch (error) {
      message/* default */.Ay.error(error.message || "登录失败，请重试");
    } finally {
      setLoginLoading(false);
    }
  };
  const handleRegister = async values => {
    setRegisterLoading(true);
    try {
      if (values.password !== values.confirmPassword) {
        message/* default */.Ay.error("两次输入的密码不一致");
        return;
      }
      const response = await api/* authAPI */.R2.register(values.username, values.password);
      if (response.status) {
        message/* default */.Ay.success("注册成功！请使用您的账号密码登录");
        // 切换到登录标签页并预填充用户名
        setActiveTab("login");
        loginForm.setFieldsValue({
          username: values.username
        });
      }
    } catch (error) {
      message/* default */.Ay.error(error.message || "注册失败，请重试");
    } finally {
      setRegisterLoading(false);
    }
  };
  return /*#__PURE__*/react.createElement("div", {
    style: backgroundStyle
  }, /*#__PURE__*/react.createElement("style", null, loginTabsStyle), /*#__PURE__*/react.createElement("div", {
    style: logoContainerStyle
  }, "IHEP\u8BA1\u7B97\u4E2D\u5FC3"), /*#__PURE__*/react.createElement("div", {
    style: containerStyle
  }, /*#__PURE__*/react.createElement("h2", {
    style: h2Style
  }, "\u6B22\u8FCE\u63A2\u7D22 Dr. Sai \u667A\u80FD\u4F53"), /*#__PURE__*/react.createElement(card/* default */.A, {
    style: cardStyle
  }, /*#__PURE__*/react.createElement(tabs/* default */.A, {
    activeKey: activeTab,
    onChange: setActiveTab,
    centered: true,
    className: "login-page-tabs"
  }, /*#__PURE__*/react.createElement(TabPane, {
    tab: "\u767B\u5F55",
    key: "login"
  }, /*#__PURE__*/react.createElement(es_form/* default */.A, {
    form: loginForm,
    name: "login",
    onFinish: handleLogin,
    autoComplete: "off",
    layout: "vertical"
  }, /*#__PURE__*/react.createElement(es_form/* default */.A.Item, {
    name: "username",
    rules: [{
      required: true,
      message: "请输入用户名!"
    }]
  }, /*#__PURE__*/react.createElement(input/* default */.A, {
    prefix: /*#__PURE__*/react.createElement(icons_UserOutlined, null),
    placeholder: "\u7528\u6237\u540D",
    size: "large"
  })), /*#__PURE__*/react.createElement(es_form/* default */.A.Item, {
    name: "password",
    rules: [{
      required: true,
      message: "请输入密码!"
    }]
  }, /*#__PURE__*/react.createElement(input/* default */.A.Password, {
    prefix: /*#__PURE__*/react.createElement(icons_LockOutlined, null),
    placeholder: "\u5BC6\u7801",
    size: "large"
  })), /*#__PURE__*/react.createElement(es_form/* default */.A.Item, null, /*#__PURE__*/react.createElement(es_button/* default */.Ay, {
    htmlType: "submit",
    size: "large",
    block: true,
    loading: loginLoading,
    className: "login-page-button"
  }, "\u767B\u5F55")))), /*#__PURE__*/react.createElement(TabPane, {
    tab: "\u6CE8\u518C",
    key: "register"
  }, /*#__PURE__*/react.createElement(es_form/* default */.A, {
    name: "register",
    onFinish: handleRegister,
    autoComplete: "off",
    layout: "vertical"
  }, /*#__PURE__*/react.createElement(es_form/* default */.A.Item, {
    name: "username",
    rules: [{
      required: true,
      message: "请输入用户名!"
    }, {
      min: 3,
      message: "用户名至少3个字符!"
    }, {
      validator: (_, value) => {
        if (!value) {
          return Promise.resolve();
        }
        // 检查是否是纯数字
        if (/^\d+$/.test(value)) {
          return Promise.reject(new Error("用户名不能是纯数字!"));
        }
        return Promise.resolve();
      }
    }]
  }, /*#__PURE__*/react.createElement(input/* default */.A, {
    prefix: /*#__PURE__*/react.createElement(icons_UserOutlined, null),
    placeholder: "\u7528\u6237\u540D",
    size: "large"
  })), /*#__PURE__*/react.createElement(es_form/* default */.A.Item, {
    name: "password",
    rules: [{
      required: true,
      message: "请输入密码!"
    }, {
      min: 6,
      message: "密码至少6个字符!"
    }]
  }, /*#__PURE__*/react.createElement(input/* default */.A.Password, {
    prefix: /*#__PURE__*/react.createElement(icons_LockOutlined, null),
    placeholder: "\u5BC6\u7801",
    size: "large"
  })), /*#__PURE__*/react.createElement(es_form/* default */.A.Item, {
    name: "confirmPassword",
    rules: [{
      required: true,
      message: "请确认密码!"
    }, _ref => {
      let {
        getFieldValue
      } = _ref;
      return {
        validator(_, value) {
          if (!value || getFieldValue('password') === value) {
            return Promise.resolve();
          }
          return Promise.reject(new Error('两次输入的密码不一致!'));
        }
      };
    }]
  }, /*#__PURE__*/react.createElement(input/* default */.A.Password, {
    prefix: /*#__PURE__*/react.createElement(icons_LockOutlined, null),
    placeholder: "\u786E\u8BA4\u5BC6\u7801",
    size: "large"
  })), /*#__PURE__*/react.createElement(es_form/* default */.A.Item, null, /*#__PURE__*/react.createElement(es_button/* default */.Ay, {
    htmlType: "submit",
    size: "large",
    block: true,
    loading: registerLoading,
    className: "login-page-button"
  }, "\u6CE8\u518C"))))))));
};
/* harmony default export */ var login = (LoginPage);

/***/ })

}]);
//# sourceMappingURL=component---src-pages-login-tsx-759fe2914aa9f4769e79.js.map