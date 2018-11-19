import React from 'react'
import './index.scss'

class FuncHead extends React.Component {
    render() {
        const {title, subtitle} = this.props
        return (
            <div className="function-header">
                <h4 className="main-title">{title}</h4>
                {
                    subtitle ? <span className="subtitle">{subtitle}</span> : null
                }
            </div>
        )
    }
}

export default FuncHead
