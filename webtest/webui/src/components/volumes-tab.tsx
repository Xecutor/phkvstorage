import * as React from "react";
import * as ReactDOM from "react-dom";

import { Table, Label, Input, Button, Transition } from 'semantic-ui-react'
import { jsonrpcCall } from "../utils/jsonrpc";

import './table-fix.css'

export interface VolumeInfo {
    volumePath: string
    volumeName: string
    mountPointPath: string
    volumeId: number
}

interface VolumesTabState {
    volumePath: string
    volumeName: string
    mountPointPath: string
}

interface VolumesTabProps {
    volumes: VolumeInfo[]
    reloadVolumes: () => void
}

export class VolumesTab extends React.Component<VolumesTabProps, VolumesTabState>{
    constructor(props: VolumesTabProps) {
        super(props)
        this.state = {
            volumePath: '.',
            volumeName: 'volumeName',
            mountPointPath: '/'
        }
    }

    onVolumePathChanged(e: any, { value }: { value: string }) {
        this.setState({ volumePath: value });
    }
    onVolumePathChangedBound = this.onVolumePathChanged.bind(this)

    onVolumeNameChanged(e: any, { value }: { value: string }) {
        this.setState({ volumeName: value })
    }
    onVolumeNameChangedBound = this.onVolumeNameChanged.bind(this)

    onMountPointPathChanged(e: any, { value }: { value: string }) {
        this.setState({ mountPointPath: value })
    }
    onMountPointPathChangedBound = this.onMountPointPathChanged.bind(this)

    onError(reason: any) {
        alert(reason)
        console.log(reason)
    }
    onErrorBound = this.onError.bind(this)

    onCreateClick() {
        jsonrpcCall("create_and_mount_volume", {
            volumePath: this.state.volumePath,
            volumeName: this.state.volumeName,
            mountPointPath: this.state.mountPointPath
        }).then(() => this.props.reloadVolumes(), this.onErrorBound)
    }
    onCreateClickBound = this.onCreateClick.bind(this)

    onMountClick() {
        jsonrpcCall("mount_volume", {
            volumePath: this.state.volumePath,
            volumeName: this.state.volumeName,
            mountPointPath: this.state.mountPointPath
        }).then(this.props.reloadVolumes, this.onErrorBound)
    }
    onMountClickBound = this.onMountClick.bind(this)

    onUnmountClick(volumeId:number) {
        jsonrpcCall("unmount_volume", {volumeId}).then(this.props.reloadVolumes, this.onErrorBound)
    }

    render() {
        return <div>
            <Label>Volume path:</Label><Input value={this.state.volumePath} onChange={this.onVolumePathChangedBound} />
            <Label>Volume name:</Label><Input value={this.state.volumeName} onChange={this.onVolumeNameChangedBound} />
            <Label>Mount point path:</Label><Input value={this.state.mountPointPath} onChange={this.onMountPointPathChangedBound} />
            <Button onClick={this.onCreateClickBound}>Create</Button><Button onClick={this.onMountClickBound}>Mount</Button>
            <Table>
                <Table.Header>
                    <Table.Row>
                        <Table.HeaderCell>Volume id</Table.HeaderCell>
                        <Table.HeaderCell>Volume path</Table.HeaderCell>
                        <Table.HeaderCell>Volume name</Table.HeaderCell>
                        <Table.HeaderCell>Mount point path</Table.HeaderCell>
                        <Table.HeaderCell></Table.HeaderCell>
                    </Table.Row>
                </Table.Header>
                <Transition.Group as={Table.Body}>
                    {
                        this.props.volumes.map(v => <Table.Row key={v.volumeId}>
                            <Table.Cell>{v.volumeId}</Table.Cell>
                            <Table.Cell>{v.volumePath}</Table.Cell>
                            <Table.Cell>{v.volumeName}</Table.Cell>
                            <Table.Cell>{v.mountPointPath}</Table.Cell>
                            <Table.Cell><Button onClick={()=>this.onUnmountClick(v.volumeId)}>Unmount</Button></Table.Cell>
                        </Table.Row>)
                    }
                </Transition.Group>
            </Table>
        </div>
    }
}