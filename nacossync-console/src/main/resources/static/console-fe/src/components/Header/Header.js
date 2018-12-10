import React from 'react'
import {connect} from "react-redux"
import {ConfigProvider} from '@alifd/next'
import LogoImage from './images/logo.svg'
import {changeLanguage} from '../../reducers/locale'

import './index.scss'

@connect(state => ({...state.locale}), {changeLanguage})
@ConfigProvider.config
class Header extends React.Component {
    constructor(props) {
        super(props)
    }

    languageSwitching() {
        const {language = 'en-US', changeLanguage} = this.props
        changeLanguage(language === 'en-US' ? 'zh-CN' : 'en-US')
    }

    render() {
        const {locale = {}, language = 'en-US'} = this.props
        const {home, docs, blog, community, languageSwitchButton} = locale
        const BASE_URL = `https://nacos.io/${language.toLocaleLowerCase()}/`
        const NAV_MENU = [{
            id: 1,
            title: home,
            link: BASE_URL
        }, {
            id: 2,
            title: docs,
            link: `${BASE_URL}docs/what-is-nacos.html`
        }, {
            id: 3,
            title: blog,
            link: `${BASE_URL}blog/index.html`
        }, {
            id: 4,
            title: community,
            link: `${BASE_URL}community/index.html`
        }]
        return (
            <header className="header-container-primary">
                <a href="/" className="logo" title="Nacos-Sync">
                    <img src={LogoImage} className="logo-img"/>
                </a>
                <span
                    className="language-switch language-switch-primary"
                    onClick={() => this.languageSwitching()}
                >{languageSwitchButton}</span>
                <ul className="nav-menu">
                    {
                        NAV_MENU.map(item => (
                            <li key={item.id}>
                                <a href={item.link} target="_blank">{item.title}</a>
                            </li>
                        ))
                    }
                </ul>
            </header>
        )
    }
}

export default Header
