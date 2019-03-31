import fusionEnUS from '@alifd/next/lib/locale/en-us';
import fusionZhCN from '@alifd/next/lib/locale/zh-cn';
import I18N from '../locales';
import { LANGUAGE_KEY, LANGUAGE_SWITCH } from '../constants';

const enUS = Object.assign({}, fusionEnUS, I18N.enUS);
const zhCN = Object.assign({}, fusionZhCN, I18N.zhCN);

const initialState = {
  language: 'en-US',
  locale: enUS,
};

const changeLanguage = language => (dispatch) => {
  const newLan = language === 'zh-CN' ? 'zh-CN' : 'en-US';
  localStorage.setItem(LANGUAGE_KEY, newLan);
  dispatch({ type: LANGUAGE_SWITCH, language: newLan, locale: newLan === 'zh-CN' ? zhCN : enUS });
};

export default (state = initialState, action) => {
  switch (action.type) {
    case LANGUAGE_SWITCH:
      return { ...state, ...action };
    default:
      return state;
  }
};

export { changeLanguage };
