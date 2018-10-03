import * as React from "react";
import * as ReactDOM from "react-dom";

import {Label, Input, Segment, Button} from 'semantic-ui-react'
import { jsonrpcCall } from "../utils/jsonrpc";

interface LookupTabState{
    keyPath:string
    type:string
    value:string
}

interface LookupTabProps{
}

export class LookupTab extends React.Component<LookupTabProps, LookupTabState>{
    constructor(props:LookupTabProps) {
        super(props)
        this.state={
            keyPath:'/key',
            type:'',
            value:''
        }
    }

    onKeyPathChanged(e:any, {value}:{value:string})
    {
        this.setState({keyPath:value});
    }
    onKeyPathChangedBound = this.onKeyPathChanged.bind(this)

    onResult({type, value}:{type:string, value:string})
    {
        this.setState({type, value})
    }
    onResultBound = this.onResult.bind(this)

    onLookupClick()
    {
        jsonrpcCall("lookup", {key:this.state.keyPath}).then(this.onResultBound)
    }
    onLookupClickBound = this.onLookupClick.bind(this)

    render() {
        return <Segment.Group>
            <Segment><Input label='Key path:'  value={this.state.keyPath} onChange={this.onKeyPathChangedBound} action={<Button onClick={this.onLookupClickBound}>Lookup</Button>}/></Segment>
            <Segment><Label>Type:</Label>{this.state.type}</Segment>
            <Segment><Label>Value:</Label>{this.state.value}</Segment>
        </Segment.Group>
    }
}
