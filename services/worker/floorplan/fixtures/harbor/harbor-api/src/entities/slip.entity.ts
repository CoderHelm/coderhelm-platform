import { Column, Entity, PrimaryColumn } from 'typeorm';

@Entity('slip')
export class Slip {
  @PrimaryColumn()
  id!: string;

  @Column()
  name!: string;
}
